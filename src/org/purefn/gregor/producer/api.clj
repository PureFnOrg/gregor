(ns org.purefn.gregor.producer.api
  (:require [clojure.spec.alpha :as s]
            [org.purefn.gregor.producer.protocol :as proto]))


;;------------------------------------------------------------------------------
;; UnsafeProducer API 
;;------------------------------------------------------------------------------

(defn send-event*
  "Send an event asynchronously and return a future which will eventually 
   contain the RecordMetadata. 

   - Returns a future wrapped in a `Success` if successful, a `Failure` if not."
  [kafka topic key value]
  (proto/send-event* kafka topic key value))


;;------------------------------------------------------------------------------
;; Producer API 
;;------------------------------------------------------------------------------

(defn send-event
  "Send an event asynchronously and return a future which will eventually 
   contain the RecordMetadata. 

   - Returns a future if successful, a `nil` if not."
  [kafka topic key value]
  (proto/send-event kafka topic key value))
