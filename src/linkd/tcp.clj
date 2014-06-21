(ns linkd.tcp
  (:use [linkd.codec :only [netty-encoder netty-decoder]])
  (:import [java.net InetSocketAddress])
  (:import [java.util.concurrent Executors])
  (:import [org.jboss.netty.bootstrap
            ClientBootstrap
            ServerBootstrap])
  (:import [org.jboss.netty.channel
            Channels
            ChannelPipelineFactory])
  (:import [org.jboss.netty.channel.socket.nio
            NioServerSocketChannelFactory
            NioClientSocketChannelFactory]))

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

(defn- start-tcp-server [port handler encoder decoder boss-pool worker-pool]
  (let [factory (NioServerSocketChannelFactory. boss-pool worker-pool)
        bootstrap (ServerBootstrap. factory)
        pipeline (create-pipeline handler encoder decoder)]
    (.setPipelineFactory bootstrap pipeline)
    (.bind bootstrap (InetSocketAddress. port))))

(defn tcp-server [port handler
                  &{:keys [encoder decoder codec boss-pool worker-pool]
                    :or {encoder nil
                         decoder nil
                         codec nil
                         boss-pool (Executors/newCachedThreadPool)
                         worker-pool (Executors/newCachedThreadPool)}}]
  (let [encoder (or encoder codec)
        decoder (or decoder codec)]
    (start-tcp-server port handler encoder decoder boss-pool worker-pool)))

(defn tcp-client [host port handler
                  &{:keys [encoder decoder codec boss-pool worker-pool]
                    :or {encoder nil
                         decoder nil
                         codec nil
                         boss-pool (Executors/newCachedThreadPool)
                         worker-pool (Executors/newCachedThreadPool)}}]
  (let [encoder (or encoder codec)
        decoder (or decoder codec)
        bootstrap (ClientBootstrap.
                    (NioClientSocketChannelFactory. boss-pool worker-pool))
        pipeline (create-pipeline handler encoder decoder)]
    (.setPipelineFactory bootstrap pipeline)
    (.. (.connect bootstrap (InetSocketAddress. host port))
      awaitUninterruptibly
      getChannel)))




