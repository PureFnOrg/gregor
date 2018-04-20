(ns org.purefn.gregor.admin.api
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            [clojure.test.check.generators :refer [recursive-gen]]
            [com.gfredericks.test.chuck.generators :refer [string-from-regex]]
            [org.purefn.kurosawa.result :refer :all]
            [org.purefn.gregor.admin.topics :as topics]
            [org.purefn.gregor.admin.protocol :as proto]))

;;------------------------------------------------------------------------------
;; UnsafeAdmin API 
;;------------------------------------------------------------------------------

(defn topic-names*
  "The names of all topics as a sequence.

   - Returns a (possibly empty) sequence of strings wrapped in a `Success` if 
   successful, `Failure` if not."
  [kadmin]
  (proto/topic-names* kadmin))

(defn topic-config*
  "The configuration parameters for the topic with the given name.

  - Returns a topic configuration map wrapped in a `Success` if the topic is 
   found, `Failure` if not."
  [kadmin name]
  (proto/topic-config* kadmin name))

(defn topic-configs*
  "The configuration parameters for all topics.

   - Returns a two-level map of topic configurations wrapped in a `Success`.
   The top-level key is the name of each topic found.  A Failure will only be
   returned only on Zookeeper client failure."
  [kadmin]
  (proto/topic-configs* kadmin))

(defn update-topic-config*
  "Update the configuration parameters for the given topic.

   - `name` The name of the target topic.
   - `config` The topic configuration to validate.

   - Returns `true` wrapped in a `Success` if successful, `Failure` if not."
  [kadmin name topic-config]
  (proto/update-topic-config* kadmin name topic-config))

(defn create-topic*
   "Create a new topic.

   - `name` The unique name of the topic.

   In addition, the following options can be given:
   - `:partitions` The number of partitions (at least 1).
   - `:topic-config` A map of topic configuration parameters.
   - `:rack-aware-mode` Rack awareness, must be one of: `:enforced` `:disabled`
   `:safe`

   - Returns `true` wrapped in a `Success` if successful, a `Failure` if not."
  [kadmin name & opts]
  (let [{:keys [partitions replication-factor topic-config rack-aware-mode]
         :or {partitions 32
              replication-factor 2
              topic-config {}
              rack-aware-mode :enforced}} opts]
    (proto/create-topic* kadmin name partitions replication-factor
                         topic-config rack-aware-mode)))

(defn delete-topic*
  "Delete the topic with the given name.

   WARNING: This is SUPER DANGEROUS, so use with extreme caution.

   Note that `delete.topic.enable=true` must be set in the `server.properties`
   broker configuration file or topic deletion will be silently ignored by the
   Kafka brokers.  Most likely this has been set to `false` in any production
   level cluster therefore disabling this API function.

   - Returns `true` wrapped in a `Success` if successful, a `Failure` if not."
  [kadmin name]
  (proto/delete-topic* kadmin name))



;;------------------------------------------------------------------------------
;; Admin API 
;;------------------------------------------------------------------------------

(defn topic-names
  "The names of all topics as a sequence.

   - Returns a (possibly empty) sequence of strings if successful, `nil` if 
   not."
  [kadmin]
  (proto/topic-names kadmin))

(defn topic-exists?
  "Whether a topic with the given name currently exists.

   - Returns `true` if the topic is found, `false` if not."
  [kadmin name]
  (proto/topic-exists? kadmin name))

(defn topic-config
  "The configuration parameters for the topic with the given name.

  - Returns a topic configuration map if the topic is found, `nil` if not."
  [kadmin name]
  (proto/topic-config kadmin name))

(defn topic-configs
  "The configuration parameters for all topics.

   - Returns a two-level map of topic configurations. The top-level key is the 
   name of each topic found. A `nil` will only be returned only on Zookeeper 
   client failure."
  [kadmin]
  (proto/topic-configs kadmin))

(defn update-topic-config
  "Update the configuration parameters for the given topic.

   - `name` The name of the target topic.
   - `config` The topic configuration to validate.

   - Returns `true` if successful, `false` if not."
  [kadmin name topic-config]
  (proto/update-topic-config kadmin name topic-config))

(defn create-topic
  "Create a new topic.

   - `name` The unique name of the topic.

   In addition, the following options can be given:
   - `:partitions` The number of partitions (at least 1).
   - `:replication-factor` The number of replicas to create for each partition.
   - `:topic-config` A map of topic configuration parameters.
   - `:rack-aware-mode` Rack awareness, must be one of: `:enforced` `:disabled`
   `:safe`

   - Returns `true` if successful, `false` if not."
  [kadmin name & opts]
  (let [{:keys [partitions replication-factor topic-config rack-aware-mode]
         :or {partitions 32
              replication-factor 2
              topic-config {}
              rack-aware-mode :enforced}} opts]
    (proto/create-topic kadmin name partitions replication-factor
                        topic-config rack-aware-mode)))

(defn delete-topic
  "Delete the topic with the given name.

   WARNING: This is SUPER DANGEROUS, so use with extreme caution.

   Note that `delete.topic.enable=true` must be set in the `server.properties`
   broker configuration file or topic deletion will be silently ignored by the
   Kafka brokers.  Most likely this has been set to `false` in any production
   level cluster therefore disabling this API function.

   - Returns `true` if successful, `false` if not."
  [kadmin name]
  (proto/delete-topic kadmin name))







;;------------------------------------------------------------------------------
;; Data Specs.
;;------------------------------------------------------------------------------

;; Protocols.
(def kafka-admin? (partial satisfies? proto/Admin))


;; Topic Name
(def name-regex #"[a-zA-Z0-9\-]+")

(def name?
  (s/spec (s/and string?
                 (partial re-matches name-regex)
                 (comp (partial >= 249) count))
          :gen #(string-from-regex name-regex)))


;; Rack Aware Mode
(def rack-aware-mode? #{:enforced :disabled :safe})


;;------------------------------------------------------------------------------
;; Function Specs.
;;------------------------------------------------------------------------------

(s/fdef topic-names
        :args (s/cat :kadmin kafka-admin?)
        :ret (s/or :s (s/coll-of string?) :f nil?))

(s/fdef topic-exists?
        :args (s/cat :kadmin kafka-admin?
                     :name name?)
        :ret (s/or :s boolean? :f nil?))

(s/fdef topic-config
        :args (s/cat :kadmin kafka-admin?
                     :name name?)
        :ret (s/or :s topics/topic-config? :f nil?))

(s/fdef create-topic
        :args (s/cat :kadmin kafka-admin?
                     :name name?
                     :partitions topics/small-pos-int?
                     :replication-factor topics/small-nat-int?
                     :config topics/topic-config?)
        :ret nil?)
