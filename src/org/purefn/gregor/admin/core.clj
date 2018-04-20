(ns org.purefn.gregor.admin.core
  "Common utilities and specs for all events."
  (:require [clojure.string :as str]
            [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            [clojure.string :as str]
            [taoensso.timbre :as log]
            [com.stuartsierra.component :as component]
            [com.gfredericks.test.chuck.generators :refer [string-from-regex]]
            [org.purefn.kurosawa.result :refer :all]
            [org.purefn.kurosawa.error :as error]
            [org.purefn.kurosawa.log.core :as klog]
            [org.purefn.kurosawa.log.api :as log-api]
            [org.purefn.kurosawa.log.protocol :as log-proto]
            [org.purefn.kurosawa.k8s :as k8s]
            [org.purefn.gregor.admin.api :as api]
            [org.purefn.gregor.admin.topics :as topics]
            [org.purefn.gregor.admin.protocol :as proto])
  (:import [java.util UUID]
           [kafka.admin AdminUtils]
           [kafka.utils ZkUtils]
           [org.I0Itec.zkclient ZkConnection ZkClient]
           [org.apache.kafka.common.errors InvalidReplicationFactorException
            TopicExistsException]))


;;------------------------------------------------------------------------------
;; Error Helpers.
;;------------------------------------------------------------------------------

(defn- reason
  "The high-level reason for the failure."
  [ex]
  (cond
    (instance? InvalidReplicationFactorException ex) ::api/replication-factor

    (instance? TopicExistsException ex) ::api/topic-exists

    :default ::api/fatal))

(def ^:private snafu
  (partial error/snafu reason ::api/reason ::api/fatal))


;;------------------------------------------------------------------------------
;; Java Interop Helper Functions. 
;;------------------------------------------------------------------------------

;; ZkUtils.
(def ^{:private true :tag kafka.utils.ZkUtils$} zk-utils-singleton
  kafka.utils.ZkUtils$/MODULE$)

(defn- zk-connect
  [^String url
   ^Integer session-timeout-ms
   ^Integer connection-timeout-ms
   ^Boolean securityEnabled]
  (.apply zk-utils-singleton url 10000 8000 false))

(defn- zk-disconnect
  [^ZkUtils zk-utils]
  (.close zk-utils))

(defn- all-tps
  [^ZkUtils zk-utils]
  (.getAllTopics zk-utils))


;; AdminUtils.
(def ^{:private true :tag kafka.admin.AdminUtils$} admin-utils-singleton
  kafka.admin.AdminUtils$/MODULE$)

(defn- tp-exists
  [^ZkUtils zk-utils ^String name]
  (.topicExists admin-utils-singleton zk-utils name))
  
(defn- delete-tp
  [^ZkUtils zk-utils ^String name]
  (.deleteTopic admin-utils-singleton zk-utils name))
;; ^^^^ Can throw
;; TopicAlreadyMarkedForDeletionException
;; AdminOperationException
;; UnknownTopicOrPartitionException

(def ^:private topic-config-type (.Topic kafka.server.ConfigType$/MODULE$))

(defn- tp-config
  [^ZkUtils zk-utils ^String name]
  (.fetchEntityConfig admin-utils-singleton zk-utils topic-config-type name))

(defn- change-tp-config
  [^ZkUtils zk-utils ^String name ^java.util.Properties config]
  (.changeTopicConfig admin-utils-singleton zk-utils name config))
;; ^^ Can throw
;; AdminOperationException
;; InvalidConfigurationException

(def ^:private rack-aware-modes
  {:enforced kafka.admin.RackAwareMode$Enforced$/MODULE$
   :disabled kafka.admin.RackAwareMode$Disabled$/MODULE$
   :safe     kafka.admin.RackAwareMode$Safe$/MODULE$})

(defn- create-tp
  [^kafka.admin.RackAwareMode rack-aware-mode
   ^ZkUtils zk-utils
   ^String name
   ^Integer partitions
   ^Integer replication-factor
   ^java.util.Properties topic-config]
  (.createTopic admin-utils-singleton zk-utils name
                partitions replication-factor topic-config rack-aware-mode))
;; ^^ Can throw
;; TopicExistsException
;; AdminOperationException
;; InvalidTopicException
;; InvalidReplicaFactorException
;; InvalidReplicaAssignmentException

(defn- as-java-props
  [topic-config]
  (let [p (java.util.Properties.)]
    (.putAll p (topics/as-properties topic-config))
    p))

;; Scala Interop.
(defn- scala-collection-as-seq
  [^scala.collection.Seq col]
  (-> (.asJavaCollection scala.collection.JavaConverters$/MODULE$ col)
      (seq)))


;;------------------------------------------------------------------------------
;; Component. 
;;------------------------------------------------------------------------------

(defrecord Admin
    [config zkclient]
  
  component/Lifecycle
  (start [this]
    (let [{:keys [config zk-utils]} this
          {:keys [::zookeepers
                  ::zk-session-timeout-ms
                  ::zk-connection-timeout-ms]} config]
      (log/info "Starting Kafka Admin.")
      (if zk-utils
        (do
          (log/warn "Kafka Admin was already started.")
          this)
        (if-let [nzk-utils (-> (attempt zk-connect
                                     (str/join "," zookeepers)
                                     zk-session-timeout-ms
                                     zk-connection-timeout-ms
                                     false)
                            (recover (snafu "Unable to connect to Zookeeper!"
                                            {::zookeepers zookeepers}))
                            (success))]
          (assoc this :zk-utils nzk-utils)
          (do
            (log/error "Failed to start Kafka Admin.")
            this)))))
  
  (stop [this]
    (let [{:keys [zk-utils]} this]
      (log/info "Stopping Kafka Admin.")
      (if (nil? zk-utils)
        (do
          (log/warn "Kafka Admin was already stopped.")
          this)
        
        (do
          (when-not (-> (attempt zk-disconnect zk-utils)
                        (recover (snafu "Failed to disconnect from Zookeeper!"
                                        {::zk-utils zk-utils}))
                        (success))
            (log/info "Kafka Admin stopped.")
            (assoc this
                   :zk-utils nil))))))

  
  ;;----------------------------------------------------------------------------
  log-proto/Logging
  (log-namespaces [_]
    ["kafka.*" "org.apache.zookeeper.*" "org.I0Itec.zclient.*"])
  
  (log-configure [this dir]
    (klog/add-component-appender :kafka (log-api/log-namespaces this)
                                 (str dir "/kafka.log")))


  ;;----------------------------------------------------------------------------
  proto/UnsafeAdmin
  (topic-names*
    [this]
    (-> (attempt all-tps (:zk-utils this))
        (proceed scala-collection-as-seq)
        (branch (snafu "Unable to lookup topic names!" {})
                (partial vec))))

  (topic-config*
    [this name]
    (-> (attempt tp-config (:zk-utils this) name)
        (branch (snafu "Unable to get topic configuration!"
                       {::topic-name name})
                topics/as-configs)))

  (topic-configs*
    [this]
    (-> (proto/topic-names* this)
        (proceed (fn [ns]
                   (-> (map (fn [n]
                              (-> (proto/topic-config* this n)
                                  (proceed (partial conj [n]))))
                            ns)
                       (flatten-results))))
        (proceed (partial into {}))
        (recover (snafu "Unable to get topic configurations!" {}))))

  (update-topic-config*
    [this name config]
    (-> (attempt change-tp-config (:zk-utils this) name (as-java-props config))
        (branch (snafu "Unable to update the topic configuration!"
                       {::topic-name name
                        ::topic-config config})
                (constantly true))))
  
  (create-topic*
    [this name partitions replication-factor topic-config rack-aware-mode]
    (let [mode (if-let [m (rack-aware-modes rack-aware-mode)]
                 (succeed m)
                 (fail (ex-info "Illegal rack aware mode!" {})))]
      (-> mode
          (proceed create-tp (:zk-utils this) name
                   partitions replication-factor (as-java-props topic-config))
          (branch (snafu "Unable to create topic!"
                         {::topic-name name
                          ::partitions partitions
                          ::replication-factor replication-factor
                          ::topic-config topic-config
                          ::rack-aware-mode rack-aware-mode})
                  (constantly true)))))
  
  (delete-topic*
    [this name]
    (-> (attempt delete-tp (:zk-utils this) name)
        (branch (snafu "Unable to delete topic!"
                       {::topic-name name})
                (constantly true))))
  
  
  ;;----------------------------------------------------------------------------
  proto/Admin
  (topic-exists?
    [this name]
    (-> (attempt tp-exists (:zk-utils this) name)
        (recover (snafu "Unable to determine if topic exists!"
                        {::topic-name name}))
        (success)))

  (topic-names
    [this]
    (-> (proto/topic-names* this)
        (success)))

  (topic-config
    [this name]
    (-> (proto/topic-config* this name)
        (success)))

  (topic-configs
    [this]
    (-> (proto/topic-configs* this)
        (success)))
  
  (update-topic-config
    [this name config]
    (-> (proto/update-topic-config* this name config)
        (success)))

  (create-topic
    [this name partitions replication-factor topic-config rack-aware-mode]
    (-> (proto/create-topic* this name partitions replication-factor
                             topic-config rack-aware-mode)
        (success)))

  (delete-topic
    [this name]
    (-> (proto/delete-topic* this name)
        (success))))


;;------------------------------------------------------------------------------
;; Configuration 
;;------------------------------------------------------------------------------

(defn default-config
  "As much of the default configuration as can be determined from the current
   runtime environment.

   - `name` The root of the ConfigMap and Secrets directory.  Defaults to 
   `kafka` if not provided.

   All environments will provide these configurations: 

   - ...
   
   Under Kubernetes, these configs will also be provided:

   - `::brokers`
   - `::zookeepers`

   If not under Kubernetes, then only these configs are provided: 

   - `::brokers` Just a single `localhost` entry.
   - `::zookeepers` Just a single `localhost:2181/kafka` entry.

   The component system is still responsible for providing or overriding any 
   missing hostnames, bucket credentials and/or namespaces."
  ([name]
   (let [extra
         (when (k8s/kubernetes?)
           {::brokers ["kafka.data"]
            ::zookeepers ["zookeeper.data:2181/kafka"]})]
     (merge {::brokers ["localhost"]
             ::zookeepers ["localhost:2181"]
             ::zk-session-timeout-ms (* 10 1000)
             ::zk-connection-timeout-ms (* 8 1000)}
          extra)))

  ([]
   (default-config "kafka")))


;;------------------------------------------------------------------------------
;; Creation
;;------------------------------------------------------------------------------
     
(defn admin    
  [config]
  (map->Admin {:config config}))


;;------------------------------------------------------------------------------
;; Specs.
;;------------------------------------------------------------------------------
