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
            [jepsen.garage.daemon :as grg]
            [jepsen.garage.s3api :as s3]
            [knossos.model :as model]
            [slingshot.slingshot :refer [try+]]))

(defn op-get [_ _] {:type :invoke, :f :read, :value nil})
(defn op-put [_ _] {:type :invoke, :f :write, :value (str (rand-int 9))})
(defn op-del [_ _] {:type :invoke, :f :write, :value nil})

(defrecord RegClient [creds]
  client/Client
  (open! [this test node]
    (let [creds (grg/creds node)]
      (info node "s3 credentials:" creds)
      (assoc this :creds creds)))
  (setup! [this test])
  (invoke! [this test op]
    (let [[k v] (:value op)]
      (case (:f op)
        :read
          (try+
            (let [value (s3/get (:creds this) k)]
              (assoc op :type :ok, :value (independent/tuple k value)))
            (catch (re-find #"Unavailable" (.getMessage %)) ex
              (assoc op :type :fail, :error [:s3-error (.getMessage ex)])))
        :write
          (try+
            (do
              (s3/put (:creds this) k v)
              (assoc op :type :ok))
            (catch (re-find #"Unavailable" (.getMessage %)) ex
              (assoc op :type :fail, :error [:s3-error (.getMessage ex)]))))))
  (teardown! [this test])
  (close! [this test]))

(defn workload
  "Tests linearizable reads and writes"
  [opts]
  {:client           (client/timeout 10 (RegClient. nil))
   :checker          (independent/checker
                       (checker/compose
                         {:linear (checker/linearizable
                                    {:model     (model/register)
                                     :algorithm :linear})
                          :timeline (timeline/html)}))
   :generator        (independent/concurrent-generator
                       (/ (:concurrency opts) 10) ; divide threads in 10 groups
                       (range)                    ; working on 10 keys
                       (fn [k]
                         (->>
                           (gen/mix [op-get op-put op-del])
                           (gen/limit (:ops-per-key opts)))))})


