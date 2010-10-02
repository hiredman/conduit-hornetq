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
  (.createSession session-factory user password false true true false 1))

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

(def producer
  (memoize
   (fn [session queue]
     (.createProducer session queue))))

(def consumer
  (memoize
   (fn [session queue]
     (.createConsumer session queue))))

(defn receive [session queue]
  (let [msg (.receive (consumer session queue))]
    [(.getStringProperty msg "id")
     [msg (.getStringProperty msg "replyTo")]]))

(defn msg->bytes [msg]
  (let [bytes (byte-array (.getBodySize msg))]
    (-> msg .getBodyBuffer (.readBytes bytes))
    bytes))

(defn hornetq-pub-no-reply [queue id]
  (fn hornetq-no-reply [bytes]
    (let [producer (producer *session* queue)
          msg (.createMessage *session* true)]
      (-> msg .getBodyBuffer (.writeBytes bytes))
      (.putStringProperty msg "id" id)
      (.send producer msg)
      [[] hornetq-no-reply])))

(defn hornetq-sg-fn [queue id]
  (fn hornetq-reply [bytes]
    (let [producer (producer *session* queue)
          msg (.createMessage *session* true)
          reply-queue (str queue ".reply." (UUID/randomUUID))
          consumer (consumer *session* queue)]
      (create-tmp-queue *session* reply-queue)
      (-> msg .getBodyBuffer (.writeBytes bytes))
      (.putStringProperty msg "id" id)
      (.putStringProperty msg "replyTo" reply-queue)
      (.send @producer msg)
      (fn []
        (let [msg (.receive consumer)]
          (ack *session* msg)
          [msg hornetq-reply])))))

(defn reply-fn [f]
  (partial (fn hornet-reply-fn [f [bytes reply-queue]]
             (let [producer (.createProducer *session* reply-queue)
                   [[new-bytes] new-f] (f bytes)
                   new-msg (.createMessage *session* true)]
               (-> new-msg .getBodyBuffer (.writeBytes new-bytes))
               (.send producer new-msg)
               (.commit *session*)
               [[] (partial hornet-reply-fn new-f)]))
           f))

(defn hornetq-pub-reply [queue id]
  (fn hornetq-reply [bytes]
    (create-queue *session* queue)
    (let [producer (producer *session* queue)
          msg (.createMessage *session* true)
          reply-queue (str queue ".reply." (UUID/randomUUID))]
      (create-tmp-queue *session* reply-queue)
      (-> msg .getBodyBuffer (.writeBytes bytes))
      (.putStringProperty msg "id" id)
      (.putStringProperty msg "replyTo" reply-queue)
      (.send producer msg)
      (.commit *session*)
      (let [msg (.receive (consumer *session* reply-queue))
            bytes (msg->bytes msg)]
        (ack *session* msg)
        [[bytes] hornetq-reply]))))

(def-arr serialize [object]
  (with-open [baos (ByteArrayOutputStream.)
              oos (ObjectOutputStream. baos)]
    (.writeObject oos object)
    (.toByteArray baos)))

(def-arr deserialize [bytes]
  (with-open [bais (ByteArrayInputStream. bytes)
              ois (ObjectInputStream. bais)]
    (.readObject ois)))

(defn a-hornetq [queue id proc]
  (let [reply-id (str id "-reply")
        id (str id)
        queue (str queue)]
    (assoc proc
      :type :hornetq
      :source queue
      :id (str id)
      :reply (hornetq-pub-reply queue reply-id)
      :no-reply (hornetq-pub-no-reply queue id)
      :scatter-gather (hornetq-sg-fn queue reply-id)
      :parts (assoc (:parts proc)
               queue {:type :hornetq
                      (str id) (:no-reply proc)
                      reply-id (reply-fn (:reply proc))}))))


(defn hornetq-run [proc session]
  (.start session)
  (try
    (let [queue (:source proc)
          _ (create-queue session queue)]
      (letfn [(next-msg [queue]
                (fn next-msg-inner [_]
                  [[(receive session queue)] next-msg-inner]))
              (handle-msg [f msg]
                (try
                  (let [msg-pair (update-in msg [1 0] msg->bytes)
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
