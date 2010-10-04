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

(def upcase-queue (str "some.q." (UUID/randomUUID)))

(def upcase (a-hornetq
             upcase-queue
             "upcase"
             (a-arr #(.toUpperCase %))))

(def evaler-queue (str "some.q." (UUID/randomUUID)))

(def evaler (a-hornetq
             evaler-queue
             "eval"
             (a-arr eval)))

(use-fixtures :once
              (fn [f]
                (binding [*session* (hornetq-session)]
                  (let [session *session*]
                    (future
                      (try
                        (hornetq-run upcase upcase-queue session)
                        (catch Exception e
                          (.printStackTrace e))))
                    (future
                      (try
                        (hornetq-run evaler evaler-queue session)
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
  (let [result (conduit-map
                (a-comp upcase
                        pass-through)
                ["foo" "bar"])]
    (is (= ["FOO" "BAR"] result)))
  (let [[result] (conduit-map
                  (a-comp evaler
                          pass-through)
                  ['(+ 1 2)])]
    (is (= 3 result)))
  (let [result (conduit-map
                (a-comp (a-select
                         :sexp evaler
                         :string upcase)
                        pass-through)
                [[:sexp '(+ 1 2)]
                 [:string "foo"]])]
    (is (= [3 "FOO"] result))))
