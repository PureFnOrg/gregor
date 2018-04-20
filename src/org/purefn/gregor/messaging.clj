(ns org.purefn.gregor.messaging
  (:require [clojure.spec.alpha :as s])
  (:import org.apache.kafka.clients.producer.ProducerRecord
           org.apache.kafka.clients.consumer.ConsumerRecord))

(deftype Message
    [msg]

  clojure.lang.Associative
  (containsKey [_ k]
    (.containsKey msg k))
  (entryAt [_ k]
    (.entryAt msg k))

  clojure.lang.Seqable
  (seq [_]
    (.seq msg))

  clojure.lang.ILookup
  (valAt [_ key]
    (.valAt msg key))
  (valAt [_ key not-found]
    (.valAt msg key not-found))

  clojure.lang.Indexed
  (nth [_ n]
    (.nth [(:key msg) (:value msg) (:topic msg)] n))
  (nth [_ n not-found]
    (.nth [(:key msg) (:value msg) (:topic msg)] n not-found))

  clojure.lang.IFn
  (invoke [_ k]
    (get msg k))
  (invoke [_ k not-found]
    (get msg k not-found))

  clojure.lang.IPersistentCollection
  (equiv [this x]
    (.equals this x))

  java.lang.Object
  (equals [this x]
    (and (= (type this) (type x))
         (= msg (.msg x))))
  (toString [_]
    (str msg))
  (hashCode [_]
    (hash msg)))

(defn message
  "Returns a kafka Message with given topic, key, value and optional
   metadata map."
  ([topic key value]
   (Message. {:topic topic :key key :value value}))
  ([topic key value meta]
   (Message. (merge meta {:topic topic :key key :value value}))))

(defn producer-record
  "Returns a ProducerRecord from a Message, or from topic key val args."
  ([^Message m]
   (ProducerRecord. (:topic m) (:key m) (:value m)))
  ([topic key value]
   (ProducerRecord. topic key value)))


(defprotocol ToMessage
  (-to-message [o] "attempt to coerce to Message"))

(extend-protocol ToMessage
  ConsumerRecord
  (-to-message [cr]
    (message (.topic cr) (.key cr) (.value cr)
             {:partition (.partition cr)
              :offset (.offset cr)
              :timestamp (.timestamp cr)})))

(defn to-message
  "Coerces arg to a Message."
  [x]
  (-to-message x))

(s/fdef to-message
        :args (s/cat :m #(satisfies? ToMessage %))
        :ret #(instance? Message %))
