(ns org.purefn.gregor.producer.protocol
  "Protocol definitions.")

(defprotocol UnsafeProducer
  "Producer operations for a cluster of Kafka brokers.

   These low-level functions do minimal error handling and return a `Result`."
  (send-event* [this topic key value]))
   
(defprotocol Producer
  "Producer operations for a cluster of Kafka brokers."
  (send-event [this topic key value]))
   
     
