(ns org.purefn.gregor.producer.core
  "Producer operations for a cluster of Kafka brokers."
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [clojure.pprint :refer [pprint]]
            [taoensso.timbre :as log]
            [com.stuartsierra.component :as component]
            [org.purefn.kurosawa.result :refer :all]
            [org.purefn.kurosawa.error :as error]
            [org.purefn.kurosawa.log.core :as klog]
            [org.purefn.kurosawa.log.api :as log-api]
            [org.purefn.kurosawa.log.protocol :as log-proto]
            [org.purefn.kurosawa.k8s :as k8s]
            [org.purefn.gregor.serdes :as serdes]
            [org.purefn.gregor.messaging :as msg]
            [org.purefn.gregor.producer.api :as api]
            [org.purefn.gregor.producer.protocol :as proto])
  (:import [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]
           [java.util UUID]))


;;------------------------------------------------------------------------------
;; Error Helpers.
;;------------------------------------------------------------------------------

(defn- reason
  "The high-level reason for the failure."
  [ex]
  ::api/fatal)

(def ^:private snafu (partial error/snafu reason ::api/reason ::api/fatal))


;;------------------------------------------------------------------------------
;; Java Interop Helper Functions.
;;------------------------------------------------------------------------------

(defn- kafka-config
  "Takes a clojury config map and converts keys and values to strings so that
   the low-level Java classes can safely consume them."
  [config]
  (->> config
       (map (fn [[k v]]
              (let [nk (name k)
                    nv (if (keyword? v)
                         (name v)
                         v)]
                [nk nv])))
       (map (fn [[k v]]
              (if (= k "bootstrap.servers")
                (->> v
                     (map (fn [[h p]] (str h ":" p)))
                     (str/join ",")
                     (vector k))
                [k v])))
       (into {})))

(defn- close-producer
  [^KafkaProducer producer]
  (.close producer))

(defn- send-record
  [^ProducerRecord record ^KafkaProducer producer]
  (.send producer record))


;;------------------------------------------------------------------------------
;; Component.
;;------------------------------------------------------------------------------

(defrecord Producer
    [config ^KafkaProducer producer]

  component/Lifecycle
  (start [this]
    (let [{:keys [config producer]} this]
      (if producer
        (do
          (log/warn "Kafka Producer was already started.")
          this)
        (let [_ (log/info "Starting Kafka Producer.")
              p (KafkaProducer. (kafka-config config)
                                (serdes/nippy-serializer)
                                (serdes/nippy-serializer))]
          (log/info "Kafka Producer started.")
          (assoc this :producer p)))))

  (stop [this]
    (let [{:keys [producer]} this]
      (log/info "Stopping Kafka Producer.")
      (if (nil? producer)
        (do
          (log/warn "Kafka Producer was already stopped.")
          this)
        (do
          (when-not (-> (attempt close-producer producer)
                        (success?))
            (log/error "Unable to disconnect from Kafka cluster!"))
          (log/info "Kafka Producer stopped.")
          (assoc this
                 :producer nil)))))


  ;;----------------------------------------------------------------------------
  log-proto/Logging
  (log-namespaces [_]
    ["org.apache.kafka.*"])

  (log-configure [this dir]
    (klog/add-component-appender :kafka (log-api/log-namespaces this)
                                 (str dir "/kafka.log")))

  ;;----------------------------------------------------------------------------
  proto/UnsafeProducer
  (send-event* [this topic key value]
    (-> (attempt msg/producer-record topic key value)
        (proceed send-record producer)
        (recover (snafu "Failed to send event to Kafka!"
                        {::api/topic topic
                         ::api/key key
                         ::api/value value}))))

  ;;----------------------------------------------------------------------------
  proto/Producer
  (send-event [this topic key value]
    (-> (api/send-event* this topic key value)
        (success))))


;;------------------------------------------------------------------------------
;; Configuration
;;------------------------------------------------------------------------------

(defn default-config
  "As much of the default configuration as can be determined from the current
   runtime environment.

   - `name` The root of the ConfigMap and Secrets directory.  Defaults to
   `kafka` if not provided.

   The generated configuration map will be populated to reasonable defaults, but
   it is worth considering and possible overriding the following:

   - `::bootstrap.servers` A sequence of host/port pairs to use for establishing
   the initial connection to the Kafka cluster.
   - `::client.id` An id string to pass to the server when making requests. The
   purpose of this is to be able to track the source of requests beyond just
   ip/port by allowing a logical application name to be included in server-side
   request logging.
   - `::acks` The number of acknowledgments the producer requires the leader to
   have received before considering a request complete. One of: `0` `1` `:all`
   - `::batch.size` The producer will attempt to batch records together into fewer
   requests whenever multiple records are being sent to the same partition. This
   helps performance on both the client and the server. This configuration
   controls the default batch size in bytes.
   - `::linger.ms` The producer groups together any records that arrive in between
   request transmissions into a single batched request. Normally this occurs
   only under load when records arrive faster than they can be sent out. However
   in some circumstances the client may want to reduce the number of requests
   even under moderate load. This setting accomplishes this by adding a small
   amount of artificial delay—that is, rather than immediately sending out a
   record the producer will wait for up to the given delay to allow other
   records to be sent so that the sends can be batched together.

   See also: https://kafka.apache.org/documentation.html#producerconfigs"
  ([name]
   (let [extra (when (k8s/kubernetes?)
                 {::bootstrap.servers [["kafka.data" 9092]]})]
     (merge {::bootstrap.servers [["localhost" 9092]]
             ::client.id (str (UUID/randomUUID))
             ::acks :all
             ::retries (Integer. 0)
             ::batch.size (Integer. 16384)
             ::linger.ms 1
             ::compression.type :none
             ::buffer.memory 33554432}
            extra)))
  ([]
   (default-config "kafka")))


;;------------------------------------------------------------------------------
;; Creation.
;;------------------------------------------------------------------------------

(defn producer
  [config]
  (map->Producer {:config config}))


;;------------------------------------------------------------------------------
;; Specs.
;;------------------------------------------------------------------------------

;; TODO - Add configuration specs.
