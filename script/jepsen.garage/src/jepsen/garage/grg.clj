(ns jepsen.garage.grg
  (:require [clojure.tools.logging :refer :all]
            [jepsen [control :as c]
             [core :as jepsen]
             [db :as db]]
            [jepsen.control.util :as cu]
            [amazonica.aws.s3 :as s3]
            [slingshot.slingshot :refer [try+]]))

; CONSTANTS -- HOW GARAGE IS SET UP

(def dir "/opt/garage")
(def data-dir (str dir "/data"))
(def meta-dir (str dir "/meta"))
(def binary (str dir "/garage"))
(def logfile (str dir "/garage.log"))
(def pidfile (str dir "/garage.pid"))

(def grg-admin-token "icanhazadmin")
(def grg-key "jepsen")
(def grg-bucket "jepsen")

; THE GARAGE DB

(defn install!
  "Download and install Garage"
  [node version]
  (c/su
    (c/trace
      (info node "installing garage" version)
      (c/exec :mkdir :-p dir)
      (let [url (str "https://garagehq.deuxfleurs.fr/_releases/" version "/x86_64-unknown-linux-musl/garage")
            cache (cu/cached-wget! url)]
        (c/exec :cp cache binary))
      (c/exec :chmod :+x binary))))

(defn configure!
  "Configure Garage"
  [node]
  (c/su
    (c/trace
      (cu/write-file!
        (str "rpc_secret = \"0fffabe52542c2b89a56b2efb7dfd477e9dafb285c9025cbdf1de7ca21a6b372\"\n"
             "rpc_bind_addr = \"0.0.0.0:3901\"\n"
             "rpc_public_addr = \"" node ":3901\"\n"
             "db_engine = \"lmdb\"\n"
             "replication_mode = \"3\"\n"
             "data_dir = \"" dir "/data\"\n"
             "metadata_dir = \"" dir "/meta\"\n"
             "[s3_api]\n"
             "s3_region = \"us-east-1\"\n"
             "api_bind_addr = \"0.0.0.0:3900\"\n"
             "[k2v_api]\n"
             "api_bind_addr = \"0.0.0.0:3902\"\n"
             "[admin]\n"
             "api_bind_addr = \"0.0.0.0:3903\"\n"
             "admin_token = \"" grg-admin-token "\"\n")
        "/etc/garage.toml"))))

(defn connect-node!
  "Connect a Garage node to the rest of the cluster"
  [test node]
  (c/trace
    (let [node-id (c/exec binary :node :id :-q)]
      (info node "node id:" node-id)
      (c/on-many (:nodes test)
                 (c/exec binary :node :connect node-id)))))

(defn configure-node!
  "Configure a Garage node to be part of a cluster layout"
  [test node]
  (c/trace
    (let [node-id (c/exec binary :node :id :-q)]
      (c/on (jepsen/primary test)
          (c/exec binary :layout :assign (subs node-id 0 16) :-c :1G :-z :dc1 :-t node)))))

(defn finalize-config!
  "Apply the layout and create a key/bucket pair in the cluster"
  [node]
  (c/trace
    (c/exec binary :layout :apply :--version 1)
    (info node "garage status:" (c/exec binary :status))
    (c/exec binary :key :create grg-key)
    (c/exec binary :bucket :create grg-bucket)
    (c/exec binary :bucket :allow :--read :--write grg-bucket :--key grg-key)
    (info node "key info: " (c/exec binary :key :info grg-key))))

(defn db
  "Garage DB for a particular version"
  [version]
  (reify db/DB
    (setup! [_ test node]
      (install! node version)
      (configure! node)
      (cu/start-daemon!
        {:logfile logfile
         :pidfile pidfile
         :chdir dir}
        binary
        :server)
      (c/exec :sleep 3)

      (jepsen/synchronize test)
      (connect-node! test node)

      (jepsen/synchronize test)
      (configure-node! test node)

      (jepsen/synchronize test)
      (when (= node (jepsen/primary test))
        (finalize-config! node)))

    (teardown! [_ test node]
      (info node "tearing down garage" version)
      (c/su
        (cu/stop-daemon! binary pidfile)
        (c/exec :rm :-rf data-dir)
        (c/exec :rm :-rf meta-dir)))

    db/LogFiles
    (log-files [_ test node]
      [logfile])))

; GARAGE S3 HELPER FUNCTIONS

(defn s3-creds
  "Get S3 credentials for node"
  [node]
  (let [key-info (c/on node (c/exec binary :key :info grg-key :--show-secret))
        [_ ak sk] (re-matches
                    #"(?s).*Key ID: (.*)\nSecret key: (.*)\nCan create.*"
                    key-info)]
    {:access-key ak
     :secret-key sk
     :endpoint (str "http://" node ":3900")
     :bucket grg-bucket
     :client-config {:path-style-access-enabled true}}))

(defn s3-get
  "Helper for GetObject"
  [creds k]
  (try+
    (-> (s3/get-object creds (:bucket creds) k)
        :input-stream
        slurp)
    (catch (re-find #"Key not found" (.getMessage %)) ex
      nil)))

(defn s3-put
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

(defn s3-list
  "Helper for ListObjects -- just lists everything in the bucket"
  [creds prefix]
  (defn list-inner [ct accum]
    (let [list-result (s3/list-objects-v2 creds
                                          {:bucket-name (:bucket creds)
                                           :prefix prefix
                                           :continuation-token ct})
          new-object-summaries (:object-summaries list-result)
          new-objects (map (fn [d] (:key d)) new-object-summaries)
          objects (concat new-objects accum)]
      (if (:truncated? list-result)
        (list-inner (:next-continuation-token list-result) objects)
        objects)))
  (list-inner nil []))
