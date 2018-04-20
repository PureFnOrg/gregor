(ns org.purefn.gregor.admin.topics
  "Common utilities and specs for all events."
  (:require [clojure.string :as str]
            [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            [com.gfredericks.test.chuck.generators :refer [string-from-regex]]
            [org.purefn.kurosawa.result :refer :all])
  (:import [java.util UUID]))

;;------------------------------------------------------------------------------
;; Configuration 
;;------------------------------------------------------------------------------

(def default-config
  "The default topic-level configurations.
   See https://kafka.apache.org/documentation/#topic-config"
  {::cleanup-policy "delete"
   ::compression-type "producer"
   ::delete-retention-ms 86400000
   ::file-delete-delay-ms 60000
   ::flush-messages 9223372036854775807
   ::flush-ms 9223372036854775807
   ::follower-replication-throttled-replicas ""
   ::index-interval-bytes 4096
   ::leader-replication-throttled-replicas ""
   ::max-message-bytes 1000012
   ::message-format-version "0.10.2"
   ::message-timestamp-difference-max-ms 9223372036854775807
   ::message-timestamp-type "CreateTime"
   ::min-cleanable-dirty-ratio 0.5            
   ::min-compaction-lag-ms 0
   ::min-insync-replicas 1
   ::preallocate false
   ::retention-bytes -1
   ::retention-ms 604800000
   ::segment-bytes 1073741824
   ::segment-index-bytes 10485760
   ::segment-jitter-ms 0
   ::segment-ms 604800000
   ::unclean-leader-election-enable true})

(defn as-properties
  "Convert a Clojure style topic configuration map into a Java properties style 
   map."
  [config]
  (->> config
       (map (fn [[k v]]
              (let [pk (str/replace (name k) #"-" ".")]
                [pk (str v)])))
       (into {})))

(def ^:private topics-ns-name (str *ns*))

(defn as-configs
  "Convert a Java properties style map into a Clojure style topic configuration 
   map."
  [properties]
  (->> properties
       (map (fn [[k ^String v]]
              (let [ck (->> (str/replace k #"\." "-")
                            (keyword topics-ns-name))
                    cv (-> (attempt (fn [] (Long. v)))
                           (recover (fn [_] (Double. v)))
                           (recover (fn [_] (cond
                                              (= "true" (str/lower-case v)) true
                                              (= "false" (str/lower-case v)) false
                                              :else v)))
                           (success))]
                [ck cv])))
       (into {})))


;;------------------------------------------------------------------------------
;; Data Specs.
;;------------------------------------------------------------------------------

(def opt-nat-int? (s/or :some nat-int? :none #{-1}))

(def small-nat-int? (s/and nat-int? (partial >= Integer/MAX_VALUE)))

(def small-pos-int? (s/and pos-int? (partial >= Integer/MAX_VALUE)))

(def norm-double? (s/and double? #(<= 0.0 % 1.0)))

(s/def ::cleanup-policy #{"delete" "compact"})

(s/def ::compression-type #{"uncompressed" "snappy" "lz4" "gzip" "producer"})

(s/def ::delete-retention-ms nat-int?)

(s/def ::file-delete-delay-ms nat-int?)

(s/def ::flush-messages nat-int?)

(s/def ::flush-ms nat-int?)

(s/def ::follower-replication-throttled-replicas string?)

(s/def ::index-interval-bytes small-nat-int?)

(s/def ::leader-replication-throttled-replicas string?) 

(s/def ::max-message-bytes small-nat-int?)

(def version-regex #"[0-9]{1,3}(\.[0-9]{1,3}){0,3}")

(def version-string? 
  (s/spec (s/and string? (partial re-matches version-regex))
          :gen #(string-from-regex version-regex)))

(s/def ::message-format-version version-string?)

(s/def ::message-timestamp-difference-max-ms nat-int?) 

(s/def ::message-timestamp-type #{"CreateTime" "LogAppendTime"})

(s/def ::min-cleanable-dirty-ratio norm-double?)

(s/def ::min-compaction-lag-ms nat-int?)

(s/def ::min-insync-replicas small-nat-int?)

(s/def ::preallocate boolean?)

(s/def ::retention-bytes opt-nat-int?)

(s/def ::retention-ms nat-int?)

(s/def ::segment-bytes small-nat-int?)

(s/def ::segment-index-bytes small-nat-int?)

(s/def ::segment-jitter-ms nat-int?)

(s/def ::segment-ms nat-int?)

(s/def ::unclean-leader-election-enable boolean?)

(def topic-config?
  (s/keys :opt [::cleanup-policy 
                ::compression-type 
                ::delete-retention-ms
                ::file-delete-delay-ms 
                ::flush-messages
                ::flush-ms
                ::follower-replication-throttled-replicas 
                ::index-interval-bytes 
                ::leader-replication-throttled-replicas
                ::max-message-bytes 
                ::message-format-version 
                ::message-timestamp-difference-max-ms 
                ::message-timestamp-type              
                ::min-cleanable-dirty-ratio 
                ::min-compaction-lag-ms 
                ::min-insync-replicas 
                ::preallocate
                ::retention-bytes 
                ::retention-ms 
                ::segment-bytes 
                ::segment-index-bytes
                ::segment-jitter-ms
                ::segment-ms
                ::unclean-leader-election-enable]))
  
(s/def ::topic-config topic-config?)

(def topic-props? (s/map-of string? some?))


;;------------------------------------------------------------------------------
;; Function Specs.
;;------------------------------------------------------------------------------

(s/fdef as-properties
        :args (s/cat :config topic-config?)
        :ret (s/map-of string? some?))

(s/fdef as-config
        :args (s/cat :props map?)
        :ret topic-config?)
