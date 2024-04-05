(ns jepsen.garage.daemon
  (:require [clojure.tools.logging :refer :all]
            [jepsen [control :as c]
             [core :as jepsen]
             [db :as db]]
            [jepsen.control.util :as cu]))

; CONSTANTS -- HOW GARAGE IS SET UP

(def base-dir "/opt/garage")
(def data-dir (str base-dir "/data"))
(def meta-dir (str base-dir "/meta"))
(def binary (str base-dir "/garage"))
(def logfile (str base-dir "/garage.log"))
(def pidfile (str base-dir "/garage.pid"))

(def admin-token "icanhazadmin")
(def access-key-id "GK8bfb6a51286071c6c9cd8bc3")
(def secret-access-key "b0be95f71c1c6f16858a9edf395078b75c12ecb6b1c03385c4ae92076e4994a3")
(def bucket-name "jepsen")

; THE GARAGE DB

(defn install!
  "Download and install Garage"
  [node version]
  (c/su
    (c/trace
      (info node "installing garage" version)
      (c/exec :mkdir :-p base-dir)
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
             "data_dir = \"" data-dir "\"\n"
             "metadata_dir = \"" meta-dir "\"\n"
             "[s3_api]\n"
             "s3_region = \"us-east-1\"\n"
             "api_bind_addr = \"0.0.0.0:3900\"\n"
             "[k2v_api]\n"
             "api_bind_addr = \"0.0.0.0:3902\"\n"
             "[admin]\n"
             "api_bind_addr = \"0.0.0.0:3903\"\n"
             "admin_token = \"" admin-token "\"\n"
             "trace_sink = \"http://192.168.56.1:4317\"\n")
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
    (c/exec binary :key :import access-key-id secret-access-key :--yes)
    (c/exec binary :bucket :create bucket-name)
    (c/exec binary :bucket :allow :--read :--write bucket-name :--key access-key-id)
    (info node "key info: " (c/exec binary :key :info access-key-id))))

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
         :chdir base-dir
         :env {:RUST_LOG "garage=debug,garage_api=trace"}}
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
        (c/exec :rm :-rf logfile)
        (c/exec :rm :-rf data-dir)
        (c/exec :rm :-rf meta-dir)))

    db/Pause
    (pause! [_ test node]
      (cu/grepkill! :stop binary))
    (resume! [_ test node]
      (cu/grepkill! :cont binary))

    db/Kill
    (kill! [_ test node]
      (cu/stop-daemon! binary pidfile))
    (start! [_ test node]
      (cu/start-daemon!
        {:logfile logfile
         :pidfile pidfile
         :chdir base-dir
         :env {:RUST_LOG "garage=debug,garage_api=trace"}}
        binary
        :server))

    db/LogFiles
    (log-files [_ test node]
      [logfile])))

(defn creds
  "Obtain Garage credentials for node"
  [node]
  {:access-key access-key-id
   :secret-key secret-access-key
   :endpoint (str "http://" node ":3900")
   :bucket bucket-name
   :client-config {:path-style-access-enabled true}})

