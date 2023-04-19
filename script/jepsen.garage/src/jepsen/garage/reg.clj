(ns jepsen.garage.reg
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [checker :as checker]
             [cli :as cli]
             [client :as client]
             [control :as c]
             [db :as db]
             [generator :as gen]
             [independent :as independent]
             [nemesis :as nemesis]
             [tests :as tests]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [jepsen.garage.grg :as grg]
            [knossos.model :as model]
            [slingshot.slingshot :refer [try+]]))

(defn op-get [_ _] {:type :invoke, :f :read, :value nil})
(defn op-put [_ _] {:type :invoke, :f :write, :value (str (rand-int 9))})
(defn op-del [_ _] {:type :invoke, :f :write, :value nil})

(defrecord RegClient [creds]
  client/Client
  (open! [this test node]
    (let [creds (grg/s3-creds node)]
      (info node "s3 credentials:" creds)
      (assoc this :creds creds)))
  (setup! [this test])
  (invoke! [this test op]
    (let [[k v] (:value op)]
      (case (:f op)
        :read
          (let [value (grg/s3-get (:creds this) k)]
            (assoc op :type :ok, :value (independent/tuple k value)))
        :write
          (do
            (grg/s3-put (:creds this) k v)
            (assoc op :type :ok)))))
  (teardown! [this test])
  (close! [this test]))

(defn workload
  "Tests linearizable reads and writes"
  [opts]
  {:client           (RegClient. nil)
   :checker          (independent/checker
                       (checker/compose
                         {:linear (checker/linearizable
                                    {:model     (model/register)
                                     :algorithm :linear})
                          :timeline (timeline/html)}))
   :generator        (independent/concurrent-generator
                       10
                       (range)
                       (fn [k]
                         (->>
                           (gen/mix [op-get op-put op-del])
                           (gen/limit (:ops-per-key opts)))))})


