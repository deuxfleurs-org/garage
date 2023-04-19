(ns jepsen.garage
  (:require
    [clojure.string :as str]
    [jepsen
     [checker :as checker]
     [cli :as cli]
     [generator :as gen]
     [nemesis :as nemesis]
     [tests :as tests]]
    [jepsen.os.debian :as debian]
    [jepsen.garage
     [grg :as grg]
     [reg :as reg]]))

(def workloads
  "A map of workload names to functions that construct workloads, given opts."
  {"reg"  reg/workload})

(def cli-opts
  "Additional command line options."
  [["-I" "--increasing-timestamps" "Garage version with increasing timestamps on PutObject"
    :default false]
   ["-r" "--rate HZ" "Approximate number of requests per second, per thread."
    :default  10
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "Must be a positive number"]]
   [nil "--ops-per-key NUM" "Maximum number of operations on any given key."
    :default  100
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer."]]
   ["-w" "--workload NAME" "Workload of test to run"
    :default "reg"
    :validate [workloads (cli/one-of workloads)]]])

(defn garage-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (let [workload ((get workloads (:workload opts)) opts)
        garage-version (if (:increasing-timestamps opts)
                         "03490d41d58576d7b3bcf977b2726d72a3a66ada"
                         "v0.8.2")]
    (merge tests/noop-test
           opts
           {:pure-generators  true
            :name             (str "garage " (name (:workload opts)))
            :os               debian/os
            :db               (grg/db garage-version)
            :client           (:client workload)
            :generator        (:generator workload)
            :nemesis          (nemesis/partition-random-halves)
            :checker          (checker/compose
                                {:perf (checker/perf)
                                 :workload (:checker workload)})
            })))


(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn garage-test
                                         :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
