(ns org.purefn.gregor.version
  (:gen-class))

(defn -main
  []
  (println (System/getProperty "gregor.version")))
