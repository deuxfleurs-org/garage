(ns jepsen.garage.s3api
  (:require [clojure.tools.logging :refer :all]
            [jepsen [control :as c]]
            [amazonica.aws.s3 :as s3]
            [slingshot.slingshot :refer [try+]]))

; GARAGE S3 HELPER FUNCTIONS

(defn get
  "Helper for GetObject"
  [creds k]
  (try+
    (-> (s3/get-object creds (:bucket creds) k)
        :input-stream
        slurp)
    (catch (re-find #"Key not found" (.getMessage %)) ex
      nil)))

(defn put
  "Helper for PutObject or DeleteObject (is a delete if value is nil)"
  [creds k v]
  (if (= v nil)
    (s3/delete-object creds
                      :bucket-name (:bucket creds)
                      :key k)
    (let [some-bytes (.getBytes v "UTF-8")
          bytes-stream (java.io.ByteArrayInputStream. some-bytes)]
      (s3/put-object creds
                     :bucket-name (:bucket creds)
                     :key k
                     :input-stream bytes-stream
                     :metadata {:content-length (count some-bytes)}))))

(defn list-inner [creds prefix ct accum]
  (let [list-result (s3/list-objects-v2 creds
                                        {:bucket-name (:bucket creds)
                                         :prefix prefix
                                         :continuation-token ct})
        new-object-summaries (:object-summaries list-result)
        new-objects (map (fn [d] (:key d)) new-object-summaries)
        objects (concat new-objects accum)]
    (if (:truncated? list-result)
      (list-inner creds prefix (:next-continuation-token list-result) objects)
      objects)))
(defn list
  "Helper for ListObjects -- just lists everything in the bucket"
  [creds prefix]
  (list-inner creds prefix nil []))
