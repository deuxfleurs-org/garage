(ns jepsen.garage.reg
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [clojure.set :as set]
            [jepsen [checker :as checker]
             [cli :as cli]
             [client :as client]
             [control :as c]
             [db :as db]
             [generator :as gen]
             [independent :as independent]
             [nemesis :as nemesis]
             [util :as util]
             [tests :as tests]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [jepsen.garage.daemon :as grg]
            [jepsen.garage.s3api :as s3]
            [knossos.model :as model]
            [slingshot.slingshot :refer [try+]]))

(defn op-get [_ _] {:type :invoke, :f :read, :value nil})
(defn op-put [_ _] {:type :invoke, :f :write, :value (str (rand-int 99))})
(defn op-del [_ _] {:type :invoke, :f :write, :value nil})

(defrecord RegClient [creds]
  client/Client
  (open! [this test node]
    (assoc this :creds (grg/creds node)))
  (setup! [this test])
  (invoke! [this test op]
    (try+
      (let [[k v] (:value op)]
        (case (:f op)
          :read
            (util/timeout
              10000
              (assoc op :type :fail, :error ::timeout)
              (let [value (s3/get (:creds this) k)]
                (assoc op :type :ok, :value (independent/tuple k value))))
          :write
            (util/timeout
              10000
              (assoc op :type :info, :error ::timeout)
              (do
                (s3/put (:creds this) k v)
                (assoc op :type :ok)))))
      (catch (re-find #"Unavailable" (.getMessage %)) ex
        (assoc op :type :info, :error ::unavailable))
      (catch (re-find #"Broken pipe" (.getMessage %)) ex
        (assoc op :type :info, :error ::broken-pipe))
      (catch (re-find #"Connection refused" (.getMessage %)) ex
        (assoc op :type :info, :error ::connection-refused))))
  (teardown! [this test])
  (close! [this test]))

(defn reg-read-after-write
  "Read-after-Write checker for register operations"
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [init {:put-values {-1 nil}
                  :put-done #{-1}
                  :put-in-progress {}
                  :read-can-contain {}
                  :bad-reads #{}}
            final (reduce
                    (fn [state op]
                      (let [current-values (set/union
                                             (set (map (fn [idx] (get (:put-values state) idx)) (:put-done state)))
                                             (set (map (fn [[_ [idx _]]] (get (:put-values state) idx)) (:put-in-progress state))))
                            read-can-contain (reduce
                                               (fn [rcc [idx v]] (assoc rcc idx (set/union current-values v)))
                                               {} (:read-can-contain state))]
                      (info "--------")
                      (info "state: " state)
                      (info "current-values: " current-values)
                      (info "read-can-contain: " read-can-contain)
                      (info "op: " op)
                      (case [(:type op) (:f op)]
                        ([:invoke :write])
                          (assoc state
                                 :read-can-contain read-can-contain
                                 :put-values (assoc (:put-values state) (:index op) (:value op))
                                 :put-in-progress (assoc (:put-in-progress state) (:process op) [(:index op) (:put-done state)]))
                        ([:ok :write])
                          (let [[index overwrites] (get (:put-in-progress state) (:process op))]
                            (assoc state
                                   :read-can-contain read-can-contain
                                   :put-in-progress (dissoc (:put-in-progress state) (:process op))
                                   :put-done
                                     (conj
                                       (set/difference (:put-done state) overwrites)
                                       index)))
                        ([:invoke :read])
                         (assoc state
                                :read-can-contain (assoc read-can-contain (:process op) current-values))
                        ([:ok :read])
                         (let [this-read-can-contain (get read-can-contain (:process op))
                               bad-reads (if (contains? this-read-can-contain (:value op))
                                           (:bad-reads state)
                                           (conj (:bad-reads state) [(:process op) (:index op) (:value op) this-read-can-contain]))]
                           (info "this-read-can-contain: " this-read-can-contain)
                           (assoc state
                                  :read-can-contain (dissoc read-can-contain (:process op))
                                  :bad-reads bad-reads))
                        state)))
                    init history)
            valid? (empty? (:bad-reads final))]
        (assoc final :valid? valid?)))))

(defn workload-common
  "Common parts of workload"
  [opts]
  {:client           (RegClient. nil)
   :generator        (independent/concurrent-generator
                       10
                       (range)
                       (fn [k]
                         (->>
                           (gen/mix [op-get op-put op-del])
                           (gen/limit (:ops-per-key opts)))))})

(defn workload1
  "Tests linearizable reads and writes"
  [opts]
  (assoc (workload-common opts)
         :checker (independent/checker
                    (checker/compose
                      {:linear (checker/linearizable
                                 {:model     (model/register)
                                  :algorithm :linear})
                       :timeline (timeline/html)}))))

(defn workload2
  "Tests CRDT reads and writes"
  [opts]
  (assoc (workload-common opts)
         :checker (independent/checker
                    (checker/compose
                      {:reg-read-after-write (reg-read-after-write)
                       :timeline (timeline/html)}))))
