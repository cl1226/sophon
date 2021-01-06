package com.scistor.compute.http

import com.google.inject.Inject
import io.netty.channel.{ChannelHandler, ChannelHandlerContext, SimpleChannelInboundHandler}
import io.netty.handler.codec.http.FullHttpRequest
import javax.inject.Singleton
import org.slf4j.LoggerFactory

@ChannelHandler.Sharable
@Singleton
class HttpChannelHandler extends SimpleChannelInboundHandler[FullHttpRequest] {

  private[this] val log = LoggerFactory.getLogger(this.getClass)

  @Inject
  private[this] var dispatcher: Dispatcher = _

  override def channelRead0(ctx: ChannelHandlerContext, req: FullHttpRequest): Unit = {
    val uri = req.uri
    val content = req.content
    val method = req.method
    log.debug("request in ,uri:{},method:{},length:{}", uri, method, content.readableBytes())
    dispatcher.dispatch(req) match {
      case Some(response) => ctx.writeAndFlush(response)
      case _ =>
    }
  }

}
