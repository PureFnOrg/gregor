(ns org.purefn.gregor.admin.protocol
  "Protocol definitions.")

(defprotocol UnsafeAdmin
   "Administration of the Kafka cluster.

    These low-level functions do minimal error handling and return a `Result`."
   (topic-names* [this])
   (topic-config* [this name])
   (topic-configs* [this])
   (update-topic-config* [this name topic-config])
   (create-topic* [this name partitions replication-factor
                   topic-config rack-aware-mode])
   (delete-topic* [this name]))

(defprotocol Admin
   "Administration of the Kafka cluster."
   (topic-names [this])
   (topic-exists? [this name])
   (topic-config [this name])
   (topic-configs [this])
   (update-topic-config [this name topic-config])
   (create-topic [this name partitions replication-factor
                  topic-config rack-aware-mode])
   (delete-topic [this name]))
