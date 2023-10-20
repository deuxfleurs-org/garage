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
     [nemesis :as grgNemesis]
     [reg :as reg]
     [set :as set]]))

(def workloads
  "A map of workload names to functions that construct workloads, given opts."
  {"reg1" reg/workload1
   "reg2" reg/workload2
   "set1" set/workload1
   "set2" set/workload2})

(def scenari
  "A map of scenari to the associated nemesis"
  {"cp" grgNemesis/scenario-cp
   "r"  grgNemesis/scenario-r})

(def patches
  "A map of patch names to Garage builds"
  {"default" "v0.9.0"
   "tsfix1" "d146cdd5b66ca1d3ed65ce93ca42c6db22defc09"
   "tsfix2" "c82d91c6bccf307186332b6c5c6fc0b128b1b2b1"})

(def cli-opts
  "Additional command line options."
  [["-p" "--patch NAME" "Garage patch to use"
    :default "default"
    :validate [patches (cli/one-of patches)]]
   ["-s" "--scenario NAME" "Nemesis scenario to run"
    :default "cp"
    :validate [scenari (cli/one-of scenari)]]
   ["-r" "--rate HZ" "Approximate number of requests per second, per thread."
    :default  10
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "Must be a positive number"]]
   [nil "--ops-per-key NUM" "Maximum number of operations on any given key."
    :default  100
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer."]]
   ["-w" "--workload NAME" "Workload of test to run"
    :default "reg1"
    :validate [workloads (cli/one-of workloads)]]])

(defn garage-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (let [workload ((get workloads (:workload opts)) opts)
        scenario ((get scenari (:scenario opts)) opts)
        garage-version (get patches (:patch opts))]
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
                                  (gen/nemesis (:generator scenario))
                                  (gen/time-limit (:time-limit opts)))
                                (gen/log "Healing cluster")
                                (gen/nemesis (:final-generator scenario))
                                (gen/log "Waiting for recovery")
                                (gen/sleep 10)
                                (gen/clients (:final-generator workload)))
            :nemesis          (:nemesis scenario)
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
