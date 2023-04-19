(ns jepsen.garage.set
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [clojure.set :as set]
            [jepsen [checker :as checker]
             [cli :as cli]
             [client :as client]
             [control :as c]
             [checker :as checker]
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

(defn op-add-rand100 [_ _] {:type :invoke, :f :add, :value (rand-int 100)})
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

(defn set-read-after-write
  "Read-after-Write checker for set operations"
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [init {:add-started #{}
                  :add-done #{}
                  :read-must-contain {}
                  :missed #{}
                  :unexpected #{}}
            final (reduce
                    (fn [state op]
                      (case [(:type op) (:f op)]
                        ([:invoke :add])
                          (assoc state :add-started (conj (:add-started state) (:value op)))
                        ([:ok :add])
                          (assoc state :add-done (conj (:add-done state) (:value op)))
                        ([:invoke :read])
                          (assoc-in state [:read-must-contain (:process op)] (:add-done state))
                        ([:ok :read])
                          (let [read-must-contain (get (:process op) (:read-must-contain state))
                                new-missed (set/difference read-must-contain (:value op))
                                new-unexpected (set/difference (:value op) (:add-started state))]
                            (assoc state
                                   :read-must-contain (dissoc (:read-must-contain state) (:process op))
                                   :missed (set/union (:missed state) new-missed),
                                   :unexpected (set/union (:unexpected state) new-unexpected)))
                         state))
                    init history)
            valid? (and (empty? (:missed final)) (empty? (:unexpected final)))]
        (assoc final :valid? valid?)))))

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
                          (->> (range)
                           (map (fn [x] {:type :invoke, :f :add, :value x}))
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
                          {:set-full (checker/set-full {:linearizable? false})
                           :set-read-after-write (set-read-after-write)
                           :timeline (timeline/html)}))
   :generator         (independent/concurrent-generator
                        10
                        (range)
                        (fn [k]
                          (gen/mix [op-add-rand100 op-read])))})


