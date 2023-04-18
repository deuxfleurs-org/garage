(ns jepsen.garage
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [cli :as cli]
                    [client :as client]
                    [control :as c]
                    [db :as db]
                    [generator :as gen]
                    [tests :as tests]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [slingshot.slingshot :refer [try+]]
            [amazonica.aws.s3 :as s3]
            [amazonica.aws.s3transfer :as s3transfer]))

(def dir "/opt/garage")
(def binary (str dir "/garage"))
(def logfile (str dir "/garage.log"))
(def pidfile (str dir "/garage.pid"))

(def grg-admin-token "icanhazadmin")
(def grg-key "jepsen")
(def grg-bucket "jepsen")
(def grg-object "1")

(defn db
  "Garage DB for a particular version"
  [version]
  (reify db/DB
    (setup! [_ test node]
      (info node "installing garage" version)
      (c/su
        (c/exec :mkdir :-p dir)
        (let [url (str "https://garagehq.deuxfleurs.fr/_releases/" version "/x86_64-unknown-linux-musl/garage")
              cache (cu/wget! url)]
          (c/exec :cp cache binary))
        (c/exec :chmod :+x binary)
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
          "/etc/garage.toml")
        (cu/start-daemon!
          {:logfile logfile
           :pidfile pidfile
           :chdir dir}
          binary
          :server)
        (Thread/sleep 100)
        (let [node-id (c/exec binary :node :id :-q)]
          (info node "node id:" node-id)
          (c/on-many (:nodes test)
                     (c/exec binary :node :connect node-id))
          (c/exec binary :layout :assign (subs node-id 0 16) :-c 1 :-z :dc1 :-t node))
        (if (= node (first (:nodes test)))
          (do
            (Thread/sleep 2000)
            (c/exec binary :layout :apply :--version 1)
            (info node "garage status:" (c/exec binary :status))
            (c/exec binary :key :new :--name grg-key)
            (c/exec binary :bucket :create grg-bucket)
            (c/exec binary :bucket :allow :--read :--write grg-bucket :--key grg-key)
            (info node "key info: " (c/exec binary :key :info grg-key))))))
    (teardown! [_ test node]
      (info node "tearing down garage" version)
      (c/su
        (cu/stop-daemon! binary pidfile)
        (c/exec :rm :-rf dir)))
    db/LogFiles
    (log-files [_ test node]
      [logfile])))

(defn op-get [_ _] {:type :invoke, :f :get-object, :value nil})
(defn op-put [_ _] {:type :invoke, :f :put-object, :value (str (rand-int 50))})
(defn op-del [_ _] {:type :invoke, :f :del-object, :value nil})

(defrecord Client [creds]
  client/Client
  (open! [this test node]
    (let [key-info (c/on node (c/exec binary :key :info grg-key))
          [_ ak sk] (re-matches
                      #"(?s).*Key ID: (.*)\nSecret key: (.*)\nCan create.*"
                      key-info)
          creds {:access-key ak
                 :secret-key sk
                 :endpoint (str "http://" node ":3900")
                 :client-config {:path-style-access-enabled true}}]
      (info node "s3 credentials:" creds)
      (assoc this :creds creds)))
  (setup! [this test])
  (invoke! [this test op]
    (case (:f op)
      :get-object (try+
                    (let [value
                          (-> (s3/get-object (:creds this) grg-bucket grg-object)
                              :input-stream
                              slurp)]
                      (assoc op :type :ok, :value value))
                    (catch (re-find #"Key not found" (.getMessage %))  ex
                      (assoc op :type :ok, :value nil)))
      :put-object
        (let [some-bytes (.getBytes (:value op) "UTF-8")
              bytes-stream (java.io.ByteArrayInputStream. some-bytes)]
          (s3/put-object (:creds this)
                         :bucket-name grg-bucket
                         :key grg-object
                         :input-stream bytes-stream
                         :metadata {:content-length (count some-bytes)})
          (assoc op :type :ok))
      :del-object
      (do
        (s3/delete-object (:creds this)
                          :bucket-name grg-bucket
                          :key grg-object)
        (assoc op :type :ok, :value nil))))
  (teardown! [this test])
  (close! [this test]))

(defn garage-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         opts
         {:pure-generators  true
          :name             "garage"
          :os               debian/os
          :db               (db "v0.8.2")
          :client           (Client. nil)
          :generator        (->> (gen/mix [op-get op-put op-del])
                                 (gen/stagger 1)
                                 (gen/nemesis nil)
                                 (gen/time-limit 20))}))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn garage-test})
                   (cli/serve-cmd))
            args))
