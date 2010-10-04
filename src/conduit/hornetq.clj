;;TODO: procs can yield multiple values, how to deal with that with
;;reply queues?
;;TODO: auto serialize
(ns conduit.hornetq
  (:use [conduit.core])
  (:import (org.hornetq.api.core TransportConfiguration SimpleString)
           (org.hornetq.api.core.client HornetQClient MessageHandler)
           (org.hornetq.core.remoting.impl.netty NettyConnectorFactory)
           (java.util UUID)
           (java.io ByteArrayOutputStream ObjectOutputStream
                    ByteArrayInputStream ObjectInputStream)
           (java.util.concurrent ConcurrentHashMap)))

(defn create-session-factory [host port]
  (-> NettyConnectorFactory
      .getName
      (TransportConfiguration. {"host" host "port" port})
      (HornetQClient/createClientSessionFactory)
      (doto (.setReconnectAttempts -1))))

(defn create-session [session-factory user password]
  (.createSession session-factory user password false true true false 1))

(defn- create-tmp-queue [session name]
  (.createTemporaryQueue session name name))

(defn- create-queue [session name]
  (try
    (.createQueue session name name)
    (catch Exception _)))

(defn- ack [session msg]
  (.acknowledge msg)
  (.commit session))

(declare *session*)

(def ^{:private true} producer
  (memoize
   (fn [session queue]
     (.createProducer session queue))))

(def ^{:private true} consumer
  (memoize
   (fn [session queue]
     (.createConsumer session queue))))

(defn- receive [session queue]
  (let [msg (.receive (consumer session queue))]
    [(.getStringProperty msg "id")
     [msg (.getStringProperty msg "replyTo")]]))

(defn- msg->bytes [msg]
  (let [bytes (byte-array (.getBodySize msg))]
    (-> msg .getBodyBuffer (.readBytes bytes))
    bytes))

(defn- bytes->msg [session bytes]
  (doto (.createMessage session true)
    (-> .getBodyBuffer (.writeBytes bytes))))

(defn serialize [object]
  (with-open [baos (ByteArrayOutputStream.)
              oos (ObjectOutputStream. baos)]
    (.writeObject oos object)
    (.toByteArray baos)))

(defn deserialize [bytes]
  (with-open [bais (ByteArrayInputStream. bytes)
              ois (ObjectInputStream. bais)]
    (.readObject ois)))


(defn- hornetq-pub-no-reply [queue id]
  (fn hornetq-no-reply [value]
    (let [producer (producer *session* queue)
          msg (bytes->msg *session* (serialize value))]
      (.putStringProperty msg "id" id)
      (.send producer msg)
      [[] hornetq-no-reply])))

(defn- hornetq-sg-fn [queue id]
  (fn hornetq-reply [value]
    (let [producer (producer *session* queue)
          msg (bytes->msg *session* (serialize value))
          reply-queue (str queue ".reply." (UUID/randomUUID))
          consumer (consumer *session* queue)]
      (create-tmp-queue *session* reply-queue)
      (.putStringProperty msg "id" id)
      (.putStringProperty msg "replyTo" reply-queue)
      (.send @producer msg)
      (fn []
        (let [msg (.receive consumer)]
          (ack *session* msg)
          [[(deserialize (msg->bytes msg))] hornetq-reply])))))

(defn- reply-fn [f]
  (partial (fn hornet-reply-fn [f [value reply-queue]]
             (let [producer (.createProducer *session* reply-queue)
                   [[new-value] new-f] (f value)
                   new-msg (bytes->msg *session* (serialize new-value))]
               (.send producer new-msg)
               (.commit *session*)
               [[] (partial hornet-reply-fn new-f)]))
           f))

(defn- hornetq-pub-reply [queue id]
  (fn hornetq-reply [value]
    (create-queue *session* queue)
    (let [producer (producer *session* queue)
          msg (bytes->msg *session* (serialize value))
          reply-queue (str queue ".reply." (UUID/randomUUID))]
      (create-tmp-queue *session* reply-queue)
      (.putStringProperty msg "id" id)
      (.putStringProperty msg "replyTo" reply-queue)
      (.send producer msg)
      (.commit *session*)
      (let [msg (.receive (consumer *session* reply-queue))
            value (deserialize (msg->bytes msg))]
        (ack *session* msg)
        [[value] hornetq-reply]))))


(defn a-hornetq
  "turn a proc into a hornetq proc that listens on a queue"
  [queue id proc]
  (let [reply-id (str id "-reply")
        id (str id)
        queue (str queue)]
    (assoc proc
      :type :hornetq
      :source queue
      :id id
      :reply (hornetq-pub-reply queue reply-id)
      :no-reply (hornetq-pub-no-reply queue id)
      :scatter-gather (hornetq-sg-fn queue reply-id)
      :parts (assoc (:parts proc)
               queue {:type :hornetq
                      id (:no-reply proc)
                      reply-id (reply-fn (:reply proc))}))))

(defn hornetq-run
  "start a single thread executing a proc"
  [proc queue session]
  (.start session)
  (try
    (create-queue session queue)
    (letfn [(next-msg [queue]
              (fn next-msg-inner [_]
                [[(receive session queue)] next-msg-inner]))
            (handle-msg [fun [id [msg-object reply-queue]]]
              (try
                (let [msg-value (deserialize (msg->bytes msg-object))
                      [_ new-fun] (fun [id [msg-value reply-queue]])]
                  (ack session msg-object)
                  [[] (partial handle-msg new-fun)])
                (catch Exception e
                  [[] fun])))]
      (binding [*session* session]
        (->> [(next-msg queue)
              (partial handle-msg
                       (partial select-fn
                                (get-in proc [:parts queue])))]
             (reduce comp-fn)
             (a-run)
             (dorun))))
    (finally
     (.stop session))))
