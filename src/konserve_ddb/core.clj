(ns konserve-ddb.core
  (:require [clojure.core.async :as async]
            [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [cognitect.anomalies :as anomalies]
            [cognitect.aws.client.api :as aws-client]
            [cognitect.aws.client.api.async :as aws]
            [konserve.protocols :as kp]
            [superv.async :as sv]
            [clojure.edn :as edn]
            [clojure.tools.logging :as log]
            [konserve.serializers :as ser])
  (:import [java.util Base64]
           [java.io PushbackReader InputStreamReader ByteArrayInputStream ByteArrayOutputStream DataInputStream DataOutputStream]
           [java.time Clock Duration Instant]
           [com.google.common.io ByteStreams]
           [net.jpountz.lz4 LZ4Factory]))

(defn anomaly?
  [result]
  (s/valid? ::anomalies/anomaly result))

(defn throttle?
  [result]
  (and (anomaly? result)
       (some? (:__type result))
       (string/includes? (:__type result) "ProvisionedThroughputExceeded")))

(defn condition-failed?
  [result]
  (and (anomaly? result)
       (string? (:message result))
       (string/includes? (:message result) "The conditional request failed")))

(defn table-not-found?
  [result]
  (and (anomaly? result)
       (string? (:__type result))
       (string/includes? (:__type result) "ResourceNotFoundException")))

(defn encode-key
  [k]
  (.encodeToString (Base64/getUrlEncoder) (.getBytes (pr-str k))))

(defn decode-key
  [k]
  (let [b (.decode (Base64/getUrlDecoder) (name k))
        reader (PushbackReader. (InputStreamReader. (ByteArrayInputStream. b) "UTF-8"))]
    (edn/read reader)))

(defn- nano-clock
  ([] (nano-clock (Clock/systemUTC)))
  ([clock]
   (let [initial-instant (.instant clock)
         initial-nanos (System/nanoTime)]
     (proxy [Clock] []
       (getZone [] (.getZone clock))
       (withZone [zone] (nano-clock (.withZone clock zone)))
       (instant [] (.plusNanos initial-instant (- (System/nanoTime) initial-nanos)))))))

(def ^:dynamic *clock* (nano-clock))

(defn- now
  ([] (now *clock*))
  ([clock] (.instant clock)))

(defn- ms
  ([begin] (ms *clock* begin))
  ([clock ^Instant begin]
   (-> (Duration/between begin (.instant clock))
       (.toNanos)
       (double)
       (/ 1000000.0))))

(defn- do-get-in
  ([store ks] (do-get-in store ks nil))
  ([{:keys [ddb-client table-name serializer read-handlers]} key attributes-to-get]
   (sv/go-try
     sv/S
     (let [key (encode-key key)
           attributes-to-get (when attributes-to-get (conj (set attributes-to-get) "rev"))]
       (loop [backoff 100]
         (log/debug :task :ddb-get-item :phase :begin :key (pr-str key))
         (let [begin (now)
               result (async/<! (aws/invoke ddb-client {:op :GetItem
                                                        :request {:TableName table-name
                                                                  :Key {"key" {:S key}}
                                                                  :AttributesToGet attributes-to-get}}))]
           (log/debug :task :ddb-get-item :phase :end :ms (ms begin))
           (cond (throttle? result)
                 (do
                   (log/warn :task :ddb-get-item :phase :throttled :backoff backoff)
                   (async/<! (async/timeout backoff))
                   (recur (min 60000 (* backoff 2))))

                 (anomaly? result)
                 (ex-info "failed to read DynamoDB" {:error result})

                 :else
                 (when-let [item (not-empty (:Item result))]
                   {:rev (-> item :rev :N (Long/parseLong))
                    :val (some->> item :val :B (kp/-deserialize serializer read-handlers))}))))))))


(defrecord DynamoDBStore [ddb-client table-name serializer read-handlers write-handlers locks]
  kp/PEDNAsyncKeyValueStore
  (-exists? [this key]
    (sv/go-try
      sv/S
      (not (empty? (:val (sv/<? sv/S (do-get-in this key ["key"])))))))

  (-get-in [this ks]
    (sv/go-try
      sv/S
      (let [[k & ks] ks]
        (get-in (:val (sv/<? sv/S (do-get-in this k))) ks))))

  (-update-in [this ks f]
    (sv/go-try
      sv/S
      (let [[k & ks] ks
            key (encode-key k)]
        (loop [backoff 100]
          (let [{:keys [rev val]} (sv/<? sv/S (do-get-in this k))
                new-val (if (empty? ks) (f val) (update-in val ks f))
                new-rev (if (some? rev) (unchecked-inc rev) 0)
                encoded-val (let [out (ByteArrayOutputStream.)]
                              (kp/-serialize serializer out write-handlers new-val)
                              (.toByteArray out))
                update (if (some? rev)
                         (let [_ (log/debug :task :ddb-update-item :phase :begin :key (pr-str key))
                               begin (now)
                               result (async/<! (aws/invoke ddb-client {:op :UpdateItem
                                                                        :request {:TableName table-name
                                                                                  :Key {"key" {:S key}}
                                                                                  :UpdateExpression "SET #val = :newval, #rev = :newrev"
                                                                                  :ConditionExpression "#rev = :oldrev"
                                                                                  :ExpressionAttributeNames {"#rev" "rev"
                                                                                                             "#val" "val"}
                                                                                  :ExpressionAttributeValues {":newval" {:B encoded-val}
                                                                                                              ":newrev" {:N (str new-rev)}
                                                                                                              ":oldrev" {:N (str rev)}}}}))]
                           (log/debug :task :ddb-update-item :phase :end :ms (ms begin))
                           result)
                         (let [_ (log/debug :task :ddb-put-item :phase :begin :key (pr-str key))
                               begin (now)
                               result (async/<! (aws/invoke ddb-client {:op :PutItem
                                                                        :request {:TableName table-name
                                                                                  :Item {"key" {:S key}
                                                                                         "val" {:B encoded-val}
                                                                                         "rev" {:N (str new-rev)}}
                                                                                  :ConditionExpression "attribute_not_exists(#key)"
                                                                                  :ExpressionAttributeNames {"#key" "key"}}}))]
                           (log/debug :task :ddb-put-item :phase :end :ms (ms begin))
                           result))]
            (cond (throttle? update)
                  (do
                    (log/warn :task (if rev :ddb-update-item :ddb-put-item)
                              :phase :throttled :backoff backoff)
                    (async/<! (async/timeout backoff))
                    (recur (min 60000 (* backoff 2))))

                  (condition-failed? update)
                  (do
                    (log/info :task (if rev :ddb-update-item :ddb-put-item)
                              :phase :condition-failed)
                    (recur 100))

                  (anomaly? update)
                  (ex-info "failed to write DynamoDB" {:error update})

                  :else
                  [(get-in val ks)
                   (get-in new-val ks)]))))))

  (-assoc-in [this ks v]
    (kp/-update-in this ks (constantly v)))

  (-dissoc [_ k]
    (sv/go-try
      sv/S
      (let [k (encode-key k)]
        (loop [backoff 100]
          (log/debug :task :ddb-delete-item :phase :begin :key (pr-str k))
          (let [begin (now)
                result (async/<! (aws/invoke ddb-client {:op :DeleteItem
                                                         :request {:TableName table-name
                                                                   :Key {"key" {:S k}}}}))]
            (log/debug :task :ddb-delete-item :phase :end :ms (ms begin))
            (cond (throttle? result)
                  (do
                    (log/debug :task :ddb-delete-item :phase :throttled :backoff backoff)
                    (async/<! (async/timeout backoff))
                    (recur (min 60000 (* backoff 2))))

                  (anomaly? result)
                  (ex-info "failed to delete from DynamoDB" {:error result})

                  :else nil)))))))

(defn lz4-serializer
  "Wrap a konserve.protocols/PStoreSerializer such that serialized values
  are compressed with LZ4.

  Optional keyword argument :factory a net.jpountz.lz4.LZ4Factory. Defaults
  to `(net.jpountz.lz4.LZ4Factory/fastestInstance)`."
  [serializer & {:keys [factory] :or {factory (LZ4Factory/fastestInstance)}}]
  (let [compressor (.fastCompressor factory)
        decompressor (.fastDecompressor factory)]
    (reify kp/PStoreSerializer
      (-serialize [_ output-stream write-handlers val]
        (let [out (ByteArrayOutputStream.)]
          (kp/-serialize serializer out write-handlers val)
          (let [data-output (DataOutputStream. output-stream)
                serialized (.toByteArray out)
                serial-length (alength serialized)]
            (.writeByte data-output 0)
            (.writeInt data-output serial-length)
            (.write data-output (.compress compressor serialized))
            (.flush data-output))))
      (-deserialize [_ read-handlers input-stream]
        (let [data-input (DataInputStream. input-stream)
              _version (let [version (.readByte data-input)]
                         (when-not (zero? version)
                           (throw (ex-info "invalid object version" {:version version}))))
              uncompressed-len (.readInt data-input)
              bytes (ByteStreams/toByteArray data-input)
              decompressed (.decompress decompressor ^"[B" bytes uncompressed-len)]
          (kp/-deserialize serializer read-handlers (ByteArrayInputStream. decompressed)))))))

(def default-serializer
  (let [factory (LZ4Factory/fastestInstance)
        fressian (ser/fressian-serializer)]
    (lz4-serializer fressian :factory factory)))

(defn empty-store
  [{:keys [region table serializer read-handlers write-handlers read-throughput write-throughput ddb-client credentials-provider]
    :or {serializer default-serializer
         read-handlers (atom {})
         write-handlers (atom {})
         read-throughput 1
         write-throughput 1}}]
  (sv/go-try
    sv/S
    (let [ddb-client (or ddb-client (aws-client/client {:api :dynamodb
                                                        :region region
                                                        :credentials-provider credentials-provider}))
          table-info (async/<! (aws/invoke ddb-client {:op :DescribeTable
                                                       :request {:TableName table}}))
          table-ok (cond (table-not-found? table-info)
                         (async/<! (aws/invoke ddb-client {:op :CreateTable
                                                           :request {:TableName table
                                                                     :AttributeDefinitions [{:AttributeName "key"
                                                                                             :AttributeType "S"}]
                                                                     :KeySchema [{:AttributeName "key"
                                                                                  :KeyType "HASH"}]
                                                                     :ProvisionedThroughput {:ReadCapacityUnits read-throughput
                                                                                             :WriteCapacityUnits write-throughput}}}))
                         (anomaly? table-info)
                         table-info

                         (or (not= [{:AttributeName "key" :AttributeType "S"}]
                                   (-> table-info :Table :AttributeDefinitions))
                             (not= [{:AttributeName "key" :KeyType "HASH"}]
                                   (-> table-info :Table :KeySchema)))
                         {::anomalies/category ::anomalies/incorrect
                          ::anomalies/message "table exists but has incompatible configuration"}

                         :else :ok)]
      (if (anomaly? table-ok)
        (ex-info "failed to create table" {:error table-ok})
        (->DynamoDBStore ddb-client table serializer read-handlers write-handlers (atom {}))))))

(defn delete-store
  [{:keys [region table ddb-client credentials-provider]}]
  (sv/go-try
    sv/S
    (let [ddb-client (or ddb-client
                         (aws-client/client {:api :dynamodb
                                             :region region
                                             :credentials-provider credentials-provider}))
          result (async/<! (aws/invoke ddb-client {:op :DeleteTable
                                                   :request {:TableName table}}))]
      (when (anomaly? result)
        (ex-info "failed to delete table" {:error result})))))

(defn connect-store
  [{:keys [region table serializer read-handlers write-handlers ddb-client credentials-provider]
    :or {serializer default-serializer
         read-handlers (atom {})
         write-handlers (atom {})}}]
  (sv/go-try
    sv/S
    (let [ddb-client (or ddb-client
                         (aws-client/client {:api                  :dynamodb
                                             :region               region
                                             :credentials-provider credentials-provider}))
          table-info (async/<! (aws/invoke ddb-client {:op :DescribeTable
                                                       :request {:TableName table}}))
          table-ok (cond (anomaly? table-info)
                         table-info

                         (or (not= [{:AttributeName "key" :AttributeType "S"}]
                                   (-> table-info :Table :AttributeDefinitions))
                             (not= [{:AttributeName "key" :KeyType "HASH"}]
                                   (-> table-info :Table :KeySchema)))
                         {::anomalies/category ::anomalies/incorrect
                          ::anomalies/message "table exists but has incompatible configuration"}

                         :else :ok)]
      (if (anomaly? table-ok)
        (ex-info "failed to connect to table" {:error table-ok})
        (->DynamoDBStore ddb-client table serializer read-handlers write-handlers (atom {}))))))