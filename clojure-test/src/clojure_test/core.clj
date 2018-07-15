(ns clojure-test.core
  (:require [cheshire.core :as json]
            [clj-http :as http]
            [jepsen [cli :as cli]
                    [control :as c]
                    [db :as db]
                    [tests :as tests]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]))

(defn db
  [version]
  (reify db/DB
         (setup! [_ test node]
                 (info node "installing db" version))
         (teardown! [_ test node]
                    (info node "tearing down db"))))

(defn etcd-test
  [opts]
  (merge tests/noop-test opts))
(defn foo
  "I don't do a whole lot."
  [x]
  (println x "Hello, World!"))

(defn -main
  [& args]
  (println "hi")
  (cli/run! (cli/single-test-cmd {:test-fn etcd-test})
            args))
