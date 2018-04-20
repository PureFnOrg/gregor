(ns org.purefn.gregor.serdes
  "Kafka Serializer, Deserializer, and Serde using nippy."
  (:require [taoensso.nippy :as nippy])
  (:import (org.apache.kafka.common.serialization
            Serializer Deserializer Serde)))


(deftype NippySerializer
  [opts]

  Serializer
  (close [_] nil)
  (configure [_ _ _] nil)
  (serialize [_ _ x] (nippy/freeze x opts)))

(defn nippy-serializer
  "Returns a Kafka Serializer using nippy from an optional map of
   nippy opts."
  ([]
   (nippy-serializer {}))
  ([opts]
   (->NippySerializer opts)))


(deftype NippyDeserializer
    [opts]

  Deserializer
  (close [_] nil)
  (configure [_ _ _] nil)
  (deserialize [_ _ x] (when x (nippy/thaw x opts))))

(defn nippy-deserializer
  "Returns a Kafka Deserializer using nippy from an optional map of
   nippy opts."
  ([]
   (nippy-deserializer {}))
  ([opts]
   (->NippyDeserializer opts)))


(deftype NippySerde
    [opts]

  Serde
  (close [_] nil)
  (configure [_ _ _] nil)
  (serializer [_] (nippy-serializer opts))
  (deserializer [_] (nippy-deserializer opts)))

(defn nippy-serde
  "Returns a Kafka Serde (a \"factory\" for serializers and
   deserializers) using Nippy from an optional map of nippy opts."
  ([]
   (nippy-serde {}))
  ([opts]
   (->NippySerde opts)))
