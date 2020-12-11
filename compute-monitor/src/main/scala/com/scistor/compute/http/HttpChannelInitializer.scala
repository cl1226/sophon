package com.scistor.compute.http

import com.google.inject.Inject
import io.netty.channel.{Channel, ChannelInitializer}
import io.netty.handler.codec.http.{HttpObjectAggregator, HttpServerCodec}


class HttpChannelInitializer extends ChannelInitializer[Channel] {

  private[this] val maxContentLength: Int = 512 * 1024

  @Inject
  private[this] var httpChannelHandler: HttpChannelHandler = _

  override def initChannel(channel: Channel): Unit = {
    channel.pipeline
      .addLast(new HttpServerCodec)
      .addLast(new HttpObjectAggregator(maxContentLength))
      .addLast(httpChannelHandler)
  }

}
