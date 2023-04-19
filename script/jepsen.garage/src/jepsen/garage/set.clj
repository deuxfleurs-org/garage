(ns jepsen.garage.set
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

(defn op-add [_ _] {:type :invoke, :f :add, :value (rand-int 100000)})
(defn op-read [_ _] {:type :invoke, :f :read, :value nil})

(defrecord SetClient [creds]
  client/Client
  (open! [this test node]
    (let [creds (grg/s3-creds node)]
      (info node "s3 credentials:" creds)
      (assoc this :creds creds)))
  (setup! [this test])
  (invoke! [this test op]
    (let [[k v] (:value op)
          prefix (str "set" k "/")]
      (case (:f op)
        :add
          (do
            (grg/s3-put (:creds this) (str prefix v) "present")
            (assoc op :type :ok))
        :read
          (let [items (grg/s3-list (:creds this) prefix)
                items-stripped (map (fn [o] (str/replace-first o prefix "")) items)
                items-set (set (map read-string items-stripped))]
            (assoc op :type :ok, :value (independent/tuple k items-set))))))
  (teardown! [this test])
  (close! [this test]))

(defn workload1
  "Tests insertions and deletions"
  [opts]
  {:client            (SetClient. nil)
   :checker           (independent/checker
                        (checker/compose
                          {:set (checker/set)
                           :timeline (timeline/html)}))
   :generator         (independent/concurrent-generator
                        10
                        (range 100)
                        (fn [k]
                          (->>
                           (gen/mix [op-add])
                           (gen/limit (:ops-per-key opts)))))
   :final-generator   (independent/sequential-generator
                        (range 100)
                        (fn [k] (gen/once op-read)))})

(defn workload2
  "Tests insertions and deletions"
  [opts]
  {:client            (SetClient. nil)
   :checker           (independent/checker
                        (checker/compose
                          {:set (checker/set-full {:linearizable? false})
                           :timeline (timeline/html)}))
   :generator         (independent/concurrent-generator
                        10
                        (range 100)
                        (fn [k]
                          (gen/mix [op-add op-read])))})


