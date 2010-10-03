(ns conduit-hornetq.test.core
  (:use [conduit.hornetq] :reload)
  (:use [clojure.test]
        [conduit.core])
  (:import [java.util UUID]))

(defn hornetq-session []
  (let [sf (create-session-factory "localhost" 5445)]
    (create-session sf "guest" "guest")))

(def hornet-proc
  (a-comp
   (a-hornetq
    (str "some.q." (UUID/randomUUID))
    "foo"
    (a-arr identity))
   pass-through))

(def upcase (a-hornetq
             (str "some.q." (UUID/randomUUID))
             "upcase"
             (a-comp deserialize
                     (a-arr #(.toUpperCase %))
                     serialize)))

(def evaler (a-hornetq
             (str "some.q." (UUID/randomUUID))
             "eval"
             (a-comp deserialize
                     (a-arr eval)
                     serialize)))

(use-fixtures :once
              (fn [f]
                (binding [*session* (hornetq-session)]
                  (let [session *session*]
                    (future
                      (try
                        (hornetq-run upcase session)
                        (catch Exception e
                          (.printStackTrace e))))
                    (future
                      (try
                        (hornetq-run evaler session)
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
                ["foo" "bar"])]
    (is (= ["FOO" "BAR"] result)))
  (let [[result] (conduit-map
                  (a-comp serialize
                          evaler
                          deserialize
                          pass-through)
                  ['(+ 1 2)])]
    (is (= 3 result)))
  (let [result (conduit-map
                (a-comp (a-par pass-through
                               serialize)
                        (a-select
                         :sexp evaler
                         :string upcase)
                        deserialize)
                [[:sexp '(+ 1 2)]
                 [:string "foo"]])]
    (is (= [3 "FOO"] result))))
