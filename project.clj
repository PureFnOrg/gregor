(defproject org.purefn/gregor "2.1.1-SNAPSHOT"
  :description "A simple low-level Kafka producer library."
  :url "https://github.com/PureFnOrg/gregor"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :min-lein-version "2.7.1"
  ;; :global-vars {*warn-on-reflection* true}
  :dependencies [[org.clojure/clojure "1.9.0-alpha16"]
                 [org.clojure/test.check "0.9.0"]
                 [com.stuartsierra/component "0.3.2"]

                 ;; purefn
                 [org.purefn/kurosawa.core "0.1.0"]
                 [org.purefn/kurosawa.log "0.1.0"]

                 ;; kafka
                 [org.apache.kafka/kafka-clients "1.0.0"]
                 [org.apache.kafka/kafka_2.12 "1.0.0"
                  :exclusions [org.slf4j/slf4j-log4j12]]

                 ;; utility
                 [com.taoensso/timbre "4.10.0"]
                 [com.taoensso/nippy "2.13.0"]]
  :profiles
  {:dev {:dependencies [[org.clojure/tools.namespace "0.2.11"]]
         :jvm-opts ["-Xmx2g"]
         :source-paths ["dev"]
         :codeina {:sources ["src"]
                   :exclude [org.purefn.gregor.version]
                   :reader :clojure
                   :target "doc/dist/latest/api"
                   :src-uri "http://github.com/PureFnOrg/gregor/blob/master/"
                   :src-uri-prefix "#L"}
         :plugins [[funcool/codeina "0.4.0"
                    :exclusions [org.clojure/clojure]]
                   [lein-ancient "0.6.10"]]}}
  :aliases {"project-version" ["run" "-m" "org.purefn.gregor.version"]})
