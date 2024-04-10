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
  {"c" grgNemesis/scenario-c
   "cp" grgNemesis/scenario-cp
   "r"  grgNemesis/scenario-r
   "pr"  grgNemesis/scenario-pr
   "cpr"  grgNemesis/scenario-cpr
   "cdp"  grgNemesis/scenario-cdp
   "dpr"  grgNemesis/scenario-dpr})

(def patches
  "A map of patch names to Garage builds"
  {"default" "v0.9.0"
   "tsfix1" "d146cdd5b66ca1d3ed65ce93ca42c6db22defc09"
   "tsfix2" "c82d91c6bccf307186332b6c5c6fc0b128b1b2b1"
   "task3a" "707442f5de416fdbed4681a33b739f0a787b7834"
   "task3b" "431b28e0cfdc9cac6c649193cf602108a8b02997"
   "task3c" "0041b013a473e3ae72f50209d8f79db75a72848b"
   "v093" "v0.9.3"
   "v1rc1" "v1.0.0-rc1"})

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
  (let [garage-version (get patches (:patch opts))
        db (grg/db garage-version)
        workload ((get workloads (:workload opts)) opts)
        scenario ((get scenari (:scenario opts)) (assoc opts :db db))]
    (merge tests/noop-test
           opts
           {:pure-generators  true
            :name             (str "garage-" (name (:patch opts)) " " (name (:workload opts)) " " (name (:scenario opts)))
            :os               debian/os
            :db               db
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
                                (gen/log "Running final generator")
                                (gen/clients (:final-generator workload))
                                (gen/log "Generators all done"))
            :nemesis          (:nemesis scenario)
            :checker          (checker/compose
                                {:perf (checker/perf (:perf scenario))
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
