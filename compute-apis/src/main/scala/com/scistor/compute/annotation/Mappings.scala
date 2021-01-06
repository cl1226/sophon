package com.scistor.compute.annotation

import com.scistor.compute.http.MappingCtx
import io.netty.handler.codec.http.FullHttpRequest
import org.slf4j.LoggerFactory

trait Mappings {

  private[this] val log = LoggerFactory.getLogger(this.getClass)

  log.info("init Mapping,name:{}", this.getClass.getSimpleName)

  def execute(req: FullHttpRequest): Option[Any] = {
    Option(run(req, new MappingCtx(req)))
  }

  def run(req: FullHttpRequest, ctx: MappingCtx): Any

}
