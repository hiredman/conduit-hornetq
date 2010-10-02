(ns conduit-hornetq.test.core
  (:use [conduit-hornetq.core] :reload)
  (:use [clojure.test]
        [conduit.core]))

(defn hornetq-session []
  (let [sf (create-session-factory "localhost" 5445)]
    (create-session sf "guest" "guest")))

(def hornet-proc
  (a-comp
   (a-hornetq
    "some.q"
    "foo"
    (a-arr identity))
   pass-through))

(deftest test-a-hornetq
  (let [[result] ((test-conduit-fn hornet-proc) (.getBytes "foo bar"))]
    (is (= "foo bar" (String. result "utf8")))))
