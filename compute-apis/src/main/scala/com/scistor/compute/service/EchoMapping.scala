package com.scistor.compute.service

import com.google.inject.Singleton
import com.scistor.compute.annotation.{HandlerMapping, Mappings}
import com.scistor.compute.http.MappingCtx
import io.netty.handler.codec.http.FullHttpRequest

@Singleton
@HandlerMapping(value = "/api/v1/echo", httpMethod = "GET")
class EchoMapping extends Mappings {

  override def run(req: FullHttpRequest, ctx: MappingCtx): Any = {
    ctx.byteValue()
  }

}
