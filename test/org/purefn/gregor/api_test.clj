(ns org.purefn.gregor.api-test
  (:require [com.stuartsierra.component :as component]
            [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            [clojure.spec.test.alpha :as stest]
            [clojure.test :refer [is testing deftest]]
            [org.purefn.kurosawa.log.core :as klog]
            [org.purefn.gregor.producer.api :refer :all]
            [org.purefn.gregor.producer.core :as producer]))

(stest/instrument [`rand-in-range])

(def system
  (let [config {}]
    (component/system-map
     :producer (producer/producer (producer/default-config)))))

(klog/init-dev-logging)

(deftest test-all
  (testing ""
    (let [started (component/start system)
          producer (:producer started)]

      ;; ...
      
      (is true)
      
      (component/stop started))))
          
    



