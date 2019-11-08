(defproject konserve-ddb "0.1.0"
  :description "FIXME: write description"
  :url "https://github.com/csm/konserve-ddb"
  :license {:name "MIT"
            :url "https://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/tools.logging "0.4.1"]
                 [io.replikativ/konserve "0.5.1"]
                 [io.replikativ/superv.async "0.2.9"]
                 [com.cognitect.aws/api "0.8.391"]
                 [com.cognitect.aws/endpoints "1.1.11.664"]
                 [com.cognitect.aws/dynamodb "746.2.533.0"]
                 [com.cognitect/anomalies "0.1.12"]
                 [net.jpountz.lz4/lz4 "1.3"]]
  :repl-options {:init-ns konserve-ddb.repl})
