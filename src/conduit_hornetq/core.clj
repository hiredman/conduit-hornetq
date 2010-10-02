(ns conduit-hornetq.core
  (:use [conduit.core])
  (:import (org.hornetq.api.core TransportConfiguration SimpleString)
           (org.hornetq.api.core.client HornetQClient MessageHandler)
           (org.hornetq.core.remoting.impl.netty NettyConnectorFactory)
           (java.util UUID)
           (java.io ByteArrayOutputStream ObjectOutputStream
                    ByteArrayInputStream ObjectInputStream)))

(defn create-session-factory [host port]
  (-> NettyConnectorFactory .getName
      (TransportConfiguration.
       {"host" host "port" port})
      (HornetQClient/createClientSessionFactory)
      (doto (.setReconnectAttempts -1))))

(defn create-session [session-factory user password]
  (.createSession session-factory
                  user
                  password
                  false
                  true
                  true
                  false
                  1))

(defn create-tmp-queue [session name]
  (.createTemporaryQueue session name name))

(defn create-queue [session name]
  (try
    (.createQueue session name name)
    (catch Exception _)))

(defn ack [session msg]
  (.acknowledge msg)
  (.commit session))

(declare *session*)

(defn hornetq-pub-no-reply [queue id]
  (let [producer (delay (.createProducer *session* queue))]
    (fn hornetq-no-reply [bytes]
      (let [msg (.createMessage *session* true)]
        (-> msg .getBodyBuffer (.writeBytes bytes))
        (.putStringProperty msg "id" id)
        (.send @producer msg)
        [[] hornetq-no-reply]))))

(defn hornetq-sg-fn [queue id]
  (let [producer (delay (.createProducer *session* queue))]
    (fn hornetq-reply [bytes]
      (let [msg (.createMessage *session* true)
            reply-queue (str queue ".reply." (UUID/randomUUID))
            consumer (.createConsumer *session* queue)]
        (create-tmp-queue *session* reply-queue)
        (-> msg .getBodyBuffer (.writeBytes bytes))
        (.putStringProperty msg "id" id)
        (.putStringProperty msg "replyTo" reply-queue)
        (.send @producer msg)
        (fn []
          (let [msg (.receive consumer)]
            (ack *session* msg)
            [msg hornetq-reply]))))))

(defn reply-fn [f]
  (partial (fn hornet-reply-fn [f [bytes reply-queue]]
             (println "reply-fn")
             (let [producer (.createProducer *session* reply-queue)
                   [[new-bytes] new-f] (f bytes)
                   new-msg (.createMessage *session* true)]
               (-> new-msg .getBodyBuffer (.writeBytes new-bytes))
               (.send producer new-msg)
               (.commit *session*)
               [[] (partial hornet-reply-fn new-f)]))
           f))

(def producer
  (memoize
   (fn [session queue]
     (.createProducer session queue))))

(defn hornetq-pub-reply [queue id]
  (fn hornetq-reply [bytes]
    (println "@hornetq-reply" queue id)
    (println (String. bytes "utf8"))
    (create-queue *session* queue)
    (println "after queue")
    (let [producer (producer *session* queue)
          msg (.createMessage *session* true)
          reply-queue (str queue ".reply." (UUID/randomUUID))
          consumer (.createConsumer *session* queue)]
      (create-tmp-queue *session* reply-queue)
      (-> msg .getBodyBuffer (.writeBytes bytes))
      (.putStringProperty msg "id" id)
      (.putStringProperty msg "replyTo" reply-queue)
      (.send producer msg)
      (.commit *session*)
      (let [msg (.receive (.createConsumer *session* reply-queue))
            bytes (byte-array (.getBodySize msg))]
        (-> msg .getBodyBuffer (.readBytes bytes))
        (ack *session* msg)
        [[bytes] hornetq-reply]))))

(defn a-hornetq [queue id proc]
  (assoc proc
    :type :hornetq
    :source queue
    :id (str id)
    :reply (hornetq-pub-reply queue (str id "-reply"))
    :no-reply (hornetq-pub-no-reply queue id)
    :scatter-gather (hornetq-sg-fn queue (str id "-reply"))
    :parts (assoc (:parts proc)
             queue {:type :hornetq
                    (str id) (:no-reply proc)
                    (str id "-reply") (reply-fn (:reply proc))})))

(defn hornetq-run [proc session]
  (try
    (.start session)
    (let [queue (:source proc)
          _ (create-queue session queue)]
      (letfn [(next-msg [queue]
                (fn next-msg-inner [_]
                  (println "@next-msg-inner")
                  (let [msg (.receive (.createConsumer session queue))]
                    (println msg)
                    [[[(.getStringProperty msg "id")
                       [msg (.getStringProperty msg "replyTo")]]]
                     next-msg-inner])))
              (handle-msg [f msg]
                (try
                  (let [bytes (byte-array (.getBodySize (first (second msg))))
                        _ (-> (second msg) first .getBodyBuffer (.readBytes bytes))
                        msg-pair (update-in msg [1 0] (constantly bytes))
                        _ (println msg-pair)
                        result (f msg-pair)
                        new-f (second result)]
                    (ack session (first (second msg)))
                    [[] (partial handle-msg new-f)])
                  (catch Exception e
                    [[] f])))]
        (binding [*session* session]
          (dorun
           (a-run
            (reduce
             comp-fn
             [(next-msg queue)
              (partial handle-msg
                       (partial select-fn
                                (get-in proc [:parts queue])))]))))))
    (finally
     (.stop session))))

(def-arr serialize [object]
  (with-open [baos (ByteArrayOutputStream.)
              oos (ObjectOutputStream. baos)]
    (.writeObject oos object)
    (.toByteArray baos)))

(def deserialize
  (a-arr (fn deserialize [bytes]
           (with-open [bais (ByteArrayInputStream. bytes)
                       ois (ObjectInputStream. bais)]
             (.readObject ois)))))
