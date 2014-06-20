(ns linkd.tcp
  (:use [linkd.codec :only [netty-encoder netty-decoder]])
  (:import [java.net InetSocketAddress])
  (:import [java.util.concurrent Executors])
  (:import [org.jboss.netty.bootstrap
            ServerBootstrap])
  (:import [org.jboss.netty.channel
            Channels
            ChannelPipelineFactory])
  (:import [org.jboss.netty.channel.socket.nio NioServerSocketChannelFactory]))

(defn- create-pipeline [handler encoder decoder]
  (reify ChannelPipelineFactory
    (getPipeline [this]
      (let [pipeline (Channels/pipeline)]
        (when-not (nil? decoder)
          (.addLast pipeline "decoder" (netty-decoder decoder)))
        (when-not (nil? encoder)
          (.addLast pipeline "encoder" (netty-encoder encoder)))
        (.addLast pipeline "handler" handler)
        pipeline))))