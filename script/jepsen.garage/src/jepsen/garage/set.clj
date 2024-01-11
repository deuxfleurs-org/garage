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
             [util :as util]
             [tests :as tests]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [jepsen.garage.daemon :as grg]
            [jepsen.garage.s3api :as s3]
            [knossos.model :as model]
            [slingshot.slingshot :refer [try+]]))

(defn op-add-rand100 [_ _] {:type :invoke, :f :add, :value (rand-int 100)})
(defn op-read [_ _] {:type :invoke, :f :read, :value nil})

(defrecord SetClient [creds]
  client/Client
  (open! [this test node]
    (assoc this :creds (grg/creds node)))
  (setup! [this test])
  (invoke! [this test op]
    (try+
      (let [[k v] (:value op)
            prefix (str "set" k "/")]
        (case (:f op)
          :add
            (util/timeout
              10000
              (assoc op :type :info, :error ::timeout)
              (do
                (s3/put (:creds this) (str prefix v) "present")
                (assoc op :type :ok)))
          :read
            (util/timeout
              10000
              (assoc op :type :fail, :error ::timeout)
              (do
                (let [items (s3/list (:creds this) prefix)]
                  (let [items-stripped (map (fn [o]
                                            (assert (str/starts-with? o prefix))
                                            (str/replace-first o prefix "")) items)
                        items-set (set (map parse-long items-stripped))]
                    (assoc op :type :ok, :value (independent/tuple k items-set))))))))
      (catch (re-find #"Unavailable" (.getMessage %)) ex
        (assoc op :type :info, :error ::unavailable))
      (catch (re-find #"Broken pipe" (.getMessage %)) ex
        (assoc op :type :info, :error ::broken-pipe))
      (catch (re-find #"Connection refused" (.getMessage %)) ex
        (assoc op :type :info, :error ::connection-refused))))
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
                          (let [read-must-contain (get (:read-must-contain state) (:process op))
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
   :final-generator   (independent/concurrent-generator
                        10
                        (range 100)
                        (fn [k]
                          (gen/phases
                            (gen/once op-read)
                            (gen/sleep 5))))})

(defn workload2
  "Tests insertions and deletions"
  [opts]
  {:client            (SetClient. nil)
   :checker           (independent/checker
                        (checker/compose
                          {:set-read-after-write (set-read-after-write)
                           ; :set-full (checker/set-full {:linearizable? false})
                           :timeline (timeline/html)}))
   :generator         (independent/concurrent-generator
                        10
                        (range)
                        (fn [k]
                          (->> (gen/mix [op-add-rand100 op-read])
                               (gen/limit (:ops-per-key opts)))))})


