(defproject conduit-hornetq "0.0.1-SNAPSHOT"
  :description "FIXME: write"
  :repositories {"jboss" "http://snapshots.jboss.org/maven2"}
  :dependencies [[org.clojure/clojure "1.2.0"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 [org.hornetq/hornetq-core-client "2.1.1.Final"]
                 [org.jboss.netty/netty "3.2.0.Final"]
                 [net.intensivesystems/arrows "1.2.0"]
                 [net.intensivesystems/conduit "0.7.0-SNAPSHOT"]]
  :dev-dependencies [[swank-clojure "1.2.1"]])
