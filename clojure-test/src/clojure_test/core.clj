(ns clojure-test.core
  (:require [clojure.tools.logging    :refer [debug info warn]]
            [clojure.java.io          :as io]
            [clojure.string           :as str]
            [jepsen.core              :as core]
            [jepsen.os.debian         :as debian]
            [jepsen.util              :refer [meh timeout]]
            [jepsen.tests             :as tests]
            [jepsen.cli               :as cli]
            [jepsen.core              :as core]
            [jepsen.control           :as c]
            [jepsen.control.net       :as net]
            [jepsen.control.util      :as cu]
            [jepsen.client            :as client]
            [jepsen.db                :as db]
            [cheshire.core            :as json]
            [clj-http.client          :as http]
            [base64-clj.core          :as base64]))

(defn install!
  [node]
  (info node "installing")
  (c/su (debian/install-jdk8!)
          (let [x (cu/wget! "http://172.18.0.1:8000/core-0.0-all.jar")]
          (info node "-hello" x))
        (cu/wget! "http://172.18.0.1:8000/config.hocon")
        )
  )
(defn db
  [version]
  (reify db/DB
         (setup! [_ test node]
                 (install! node)
                 )
         (teardown! [_ test node]
                    (info node "tearing down db"))))

(defn etcd-test
  [opts]
  (merge tests/noop-test
         opts
         {
           :name "kv"
           :os debian/os
           :db (db "0.0")
           }))
(defn foo
  "I don't do a whole lot."
  [x]
  (println x "Hello, World!"))

(defn -main
  [& args]
  (println "hi")
  (cli/run! (cli/single-test-cmd {:test-fn etcd-test})
            args))
