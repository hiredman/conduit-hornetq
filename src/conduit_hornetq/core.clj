(ns conduit-hornetq.core
  (:use [conduit.core])
  (:import (org.hornetq.api.core TransportConfiguration SimpleString)
           (org.hornetq.api.core.client HornetQClient MessageHandler)
           (org.hornetq.core.remoting.impl.netty NettyConnectorFactory)
           (java.util UUID)))

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
                  5))

(defn create-queue [session name]
  (try
    (.createQueue session name name true)
    (catch Exception _)))

(defn create-tmp-queue [session name]
  (.createTemporaryQueue session name name))

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
            (ack msg)
            [msg hornetq-reply]))))))

(defn reply-fn [f]
  (partial (fn hornet-reply-fn [f msg]
             (let [bytes (byte-array (.getBodySize msg))
                   reply-queue (.getStringProperty msg "replyTo")]
               (-> msg .getBodyBuffer (.readBytes bytes))
               (let [producer (.createMessage *session* reply-queue)
                     [new-bytes new-f] (f bytes)
                     new-msg (.createMessage *session* true)]
                 (-> msg .getBodyBuffer (.writeBytes new-bytes))
                 (.send producer msg)
                 [[] (partial hornet-reply-fn new-f)])))
           f))

(defn hornetq-pub-reply [queue id]
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
        (let [msg (.receive consumer)]
          (ack msg)
          [msg hornetq-reply])))))

(defn a-hornetq [queue id proc]
  (assoc proc
    :type :hornetq
    :source queue
    :id (str id)
    :reply (hornetq-pub-reply queue id)
    :no-reply (hornetq-pub-no-reply queue id)
    :scatter-gather (hornetq-sg-fn queue id)
    :parts (update-in proc [:parts]
                      conj
                      [queue {:type :hornetq
                              (str id) (:no-reply proc)
                              (str id "-reply") (reply-fn (:reply proc))}])))

(defn hornetq-run [proc session queue]
  (create-queue queue)
  (let [consumer (.createConsumer session queue)]
    (letfn [(next-msg [queue]
              [(.receive consumer) next-msg])
            (handle-msg [f msg]
              (try
                (let [new-f (second (f msg))]
                  (ack msg)
                  [[] (partial handle-msg new-f)])
                (catch Exception e
                  [[] f])))]
      (binding [*session* session]
          (dorun
           (a-run
            (reduce
             comp-fn
             [(next-msg queue)
              (partial handle-msg select-fn (-> proc :parts queue))])))))))
