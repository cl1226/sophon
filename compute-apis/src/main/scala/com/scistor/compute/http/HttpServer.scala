package com.scistor.compute.http

import com.google.inject.{Inject, Singleton}
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.{AdaptiveRecvByteBufAllocator, ChannelOption}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioServerSocketChannel
import org.slf4j.LoggerFactory

@Singleton
class HttpServer {
  private[this] val log = LoggerFactory.getLogger(this.getClass)

  private[this] val bossGroup = new NioEventLoopGroup()

  private[this] val workerGroup = new NioEventLoopGroup()

  @Inject
  private[this] var channelIn: HttpChannelInitializer = _


  @throws[InterruptedException]
  def start(): Unit = {
    try {
      log.info("------ http server starting..")
      val serverBootstrap = new ServerBootstrap
      serverBootstrap
        .group(bossGroup, workerGroup)
        .channel(classOf[NioServerSocketChannel])
        .childHandler(channelIn)
        .option(ChannelOption.SO_BACKLOG.asInstanceOf[ChannelOption[Any]], 128)
        .childOption(ChannelOption.SO_KEEPALIVE.asInstanceOf[ChannelOption[Any]], true)
        .option(ChannelOption.RCVBUF_ALLOCATOR, new AdaptiveRecvByteBufAllocator(Constant.MIN_BUFFER_SIZE, Constant.INITIAL_BUFFER_SIZE, Constant.MAXIMUM_BUFFER_SIZE));
      val channelFuture = serverBootstrap.bind(Constant.iNetPort).sync
      log.info("------http server started,port:" + Constant.iNetPort)
      channelFuture.channel.closeFuture.sync
    } finally {
      workerGroup.shutdownGracefully()
      bossGroup.shutdownGracefully()
    }
  }
}
