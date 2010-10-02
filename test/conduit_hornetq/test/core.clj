(ns conduit-hornetq.test.core
  (:use [conduit-hornetq.core] :reload)
  (:use [clojure.test]
        [conduit.core]))

(def Q (gensym))

(defn hornetq-session []
  (let [sf (create-session-factory "localhost" 5445)]
    (create-session sf "guest" "guest")))

(def hornet-proc
  (a-comp
   (a-hornetq
    (str "some.q." Q)
    "foo"
    (a-arr identity))
   pass-through))

(def upcase (a-hornetq
             (str "some.q." Q)
             "upcase"
             (a-comp deserialize
                     (a-arr #(.toUpperCase %))
                     serialize)))

(use-fixtures :once
              (fn [f]
                (binding [*session* (hornetq-session)]
                  (let [session *session*]
                    (future
                      (try
                        (hornetq-run upcase session)
                        (catch Exception e
                          (.printStackTrace e)))))
                  (try
                    (.start *session*)
                    (f)
                    (finally
                     (.stop *session*))))))

(deftest test-a-hornetq
  (let [[result] ((test-conduit-fn hornet-proc) (.getBytes "foo bar"))]
    (is (= "foo bar" (String. result "utf8"))))
  (let [[result] ((test-conduit-fn
                   (a-comp serialize
                           hornet-proc
                           deserialize)) "foo bar")]
    (is (= "foo bar" result)))
  (let [result (conduit-map
                (a-comp serialize
                        upcase
                        deserialize
                        pass-through)
                ["foo"])]
    (is (= ["FOO"] result))))
