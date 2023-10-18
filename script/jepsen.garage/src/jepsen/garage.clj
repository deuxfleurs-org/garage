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
     [daemon :as grg]
     [reg :as reg]
     [set :as set]]))

(def workloads
  "A map of workload names to functions that construct workloads, given opts."
  {"reg"  reg/workload
   "set1" set/workload1
   "set2" set/workload2})

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
                         "d146cdd5b66ca1d3ed65ce93ca42c6db22defc09"
                         "v0.9.0")]
    (merge tests/noop-test
           opts
           {:pure-generators  true
            :name             (str "garage " (name (:workload opts)))
            :os               debian/os
            :db               (grg/db garage-version)
            :client           (:client workload)
            :generator        (gen/phases
                                (->>
                                  (:generator workload)
                                  (gen/stagger (/ (:rate opts)))
                                  (gen/nemesis
                                    (cycle [(gen/sleep 5)
                                            ;{:type :info, :f :partition-start}
                                            ;(gen/sleep 5)
                                            {:type :info, :f :clock-scramble}
                                            (gen/sleep 5)
                                            ;{:type :info, :f :partition-stop}
                                            ;(gen/sleep 5)
                                            {:type :info, :f :clock-scramble}]))
                                  (gen/time-limit (:time-limit opts)))
                                (gen/log "Healing cluster")
                                (gen/nemesis (gen/once {:type :info, :f :partition-stop}))
                                (gen/log "Waiting for recovery")
                                (gen/sleep 10)
                                (gen/clients (:final-generator workload)))
            :nemesis          (nemesis/compose
                                {{:partition-start :start
                                  :partition-stop :stop} (nemesis/partition-random-halves)
                                 {:clock-scramble :scramble} (nemesis/clock-scrambler 20.0)})
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
