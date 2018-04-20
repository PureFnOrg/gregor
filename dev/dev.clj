(ns dev
  (:require [com.stuartsierra.component :as component]
            [clojure.tools.namespace.repl :refer [refresh refresh-all]]
            [taoensso.timbre :as log]
            [org.purefn.kurosawa.log.core :as klog]
            [org.purefn.gregor.producer.core :as producer]
            [org.purefn.gregor.admin.core :as admin]))

(defn default-system
  []
  (component/system-map
   :admin (admin/admin (admin/default-config))
   :producer (producer/producer (producer/default-config))))

(def system
  "A Var containing an object representing the application under
  development."
  nil)

(defn init
  "Constructs the current development system."
  []
  (alter-var-root #'system
    (constantly (default-system))))

(defn start
  "Starts the current development system."
  []
  (alter-var-root #'system component/start))

(defn stop
  "Shuts down and destroys the current development system."
  []
  (alter-var-root #'system
    (fn [s] (when s (component/stop s)))))

(defn go
  "Initializes and starts the system running."
  []
  (init)
  #_(klog/init-dev-logging)         ;; high-level and low-level to console.
  (klog/init-dev-logging system [])    ;; high-level only.
  #_(klog/init-prod-logging system) ;; high-level to console, low-level to files.
  #_(klog/set-level :debug)
  (start)
  :ready)

(defn reset
  "Stops the system, reloads modified source files, and restarts it."
  []
  (stop)
  (refresh :after `go))
