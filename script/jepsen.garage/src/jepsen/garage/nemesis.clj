(ns jepsen.garage.nemesis
  (:require [clojure.tools.logging :refer :all]
            [jepsen [control :as c]
             [core :as jepsen]
             [generator :as gen]
             [nemesis :as nemesis]]
            [jepsen.nemesis.combined :as combined]
            [jepsen.garage.daemon :as grg]
            [jepsen.control.util :as cu]))

; ---- reconfiguration nemesis ----

(defn configure-present!
  "Configure node to be active in new cluster layout"
  [test nodes]
  (info "configure-present!" nodes)
  (let [node-ids (c/on-many nodes (c/exec grg/binary :node :id :-q))
        node-id-strs (map (fn [[_ v]] (subs v  0 16)) node-ids)]
    (c/on
      (jepsen/primary test)
      (apply c/exec (concat [grg/binary :layout :assign :-c :1G] node-id-strs)))))

(defn configure-absent!
  "Configure nodes to be active in new cluster layout"
  [test nodes]
  (info "configure-absent!" nodes)
  (let [node-ids (c/on-many nodes (c/exec grg/binary :node :id :-q))
        node-id-strs (map (fn [[_ v]] (subs v  0 16)) node-ids)]
    (c/on
      (jepsen/primary test)
      (apply c/exec (concat [grg/binary :layout :assign :-g] node-id-strs)))))

(defn finalize-config!
  "Apply the proposed cluster layout"
  [test]
  (let [layout-show (c/on (jepsen/primary test) (c/exec grg/binary :layout :show))
        [_ layout-next-version] (re-find #"apply --version (\d+)\n" layout-show)]
    (if layout-next-version
      (do
        (info "layout show: " layout-show "; next-version: " layout-next-version)
        (c/on (jepsen/primary test)
              (c/exec grg/binary :layout :apply :--version layout-next-version)))
      (info "no layout changes to apply"))))

(defn reconfigure-subset
  "Reconfigure cluster with only a subset of nodes"
  [cnt]
  (reify nemesis/Nemesis
    (setup! [this test] this)

    (invoke! [this test op] op
      (case (:f op)
        :start
          (let [[keep-nodes remove-nodes]
                (->> (:nodes test)
                     shuffle
                     (split-at cnt))]
            (info "layout split: keep " keep-nodes ", remove " remove-nodes)
            (configure-present! test keep-nodes)
            (configure-absent! test remove-nodes)
            (finalize-config! test)
            (assoc op :value keep-nodes))
        :stop
          (do
            (info "layout un-split: all nodes=" (:nodes test))
            (configure-present! test (:nodes test))
            (finalize-config! test)
            (assoc op :value (:nodes test)))))

    (teardown! [this test] this)))

; ---- nemesis scenari ----

(defn nemesis-op
  "A generator for a single nemesis operation"
  [op]
  (fn [_ _] {:type :info, :f op}))

(defn reconfiguration-package
  "Cluster reconfiguration nemesis package"
  [opts]
  {:generator        (->>
                       (gen/mix [(nemesis-op :reconfigure-start)
                                 (nemesis-op :reconfigure-stop)])
                       (gen/stagger (:interval opts 5)))
   :final-generator  {:type :info, :f :reconfigure-stop}
   :nemesis          (nemesis/compose
                       {{:reconfigure-start :start
                         :reconfigure-stop :stop} (reconfigure-subset 3)})
   :perf              #{{:name  "reconfigure"
                         :start #{:reconfigure-start}
                         :stop  #{:reconfigur-stop}
                         :color "#A197E9"}}})

(defn scenario-c
  "Clock modifying scenario"
  [opts]
  (combined/clock-package {:db (:db opts), :interval 1, :faults #{:clock}}))

(defn scenario-cp
  "Clock modifying + partition scenario"
  [opts]
  (combined/compose-packages
    [(combined/clock-package {:db (:db opts), :interval 1, :faults #{:clock}})
     (combined/partition-package {:db (:db opts), :interval 1, :faults #{:partition}})]))

(defn scenario-r
  "Cluster reconfiguration scenario"
  [opts]
  (reconfiguration-package {:interval 1}))

(defn scenario-pr
  "Partition + cluster reconfiguration scenario"
  [opts]
  (combined/compose-packages
    [(combined/partition-package {:db (:db opts), :interval 1, :faults #{:partition}})
     (reconfiguration-package {:interval 1})]))

(defn scenario-cpr
  "Clock scramble + partition + cluster reconfiguration scenario"
  [opts]
  (combined/compose-packages
    [(combined/clock-package {:db (:db opts), :interval 1, :faults #{:clock}})
     (combined/partition-package {:db (:db opts), :interval 1, :faults #{:partition}})
     (reconfiguration-package {:interval 1})]))

(defn scenario-cdp
  "Clock modifying + db + partition scenario"
  [opts]
  (combined/compose-packages
    [(combined/clock-package {:db (:db opts), :interval 1, :faults #{:clock}})
     (combined/db-package {:db (:db opts), :interval 1, :faults #{:db :pause :kill}})
     (combined/partition-package {:db (:db opts), :interval 1, :faults #{:partition}})]))

(defn scenario-dpr
  "Db + partition + cluster reconfiguration scenario"
  [opts]
  (combined/compose-packages
    [(combined/db-package {:db (:db opts), :interval 1, :faults #{:db :pause :kill}})
     (combined/partition-package {:db (:db opts), :interval 1, :faults #{:partition}})
     (reconfiguration-package {:interval 1})]))

