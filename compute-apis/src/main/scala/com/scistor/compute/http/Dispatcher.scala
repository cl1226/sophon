package com.scistor.compute.http

import java.nio.charset.Charset
import com.alibaba.fastjson.JSON
import com.google.inject.{Inject, Singleton}
import com.scistor.compute.annotation.Mappings
import com.scistor.compute.common.MappingScanner
import io.netty.buffer.{ByteBuf, Unpooled, UnpooledByteBufAllocator}
import io.netty.handler.codec.http.{HttpHeaderNames, _}
import io.netty.util.CharsetUtil
import org.slf4j.LoggerFactory

import scala.collection.mutable

@Singleton
class Dispatcher {


  private[this] val log = LoggerFactory.getLogger(this.getClass)

  private[this] val allocator = UnpooledByteBufAllocator.DEFAULT

  private[this] var mapping: mutable.Set[MappingCase] = _

  @Inject
  def this(scanner: MappingScanner) {
    this()
    mapping = scanner.routerMapping(Constant.mappingPackage)
  }


  def dispatch(req: FullHttpRequest): Option[DefaultFullHttpResponse] = {
    log.debug("dispatch uri :{}", req.uri())
    mapping
      .find(x => x.pattern.matches(req.uri()) && x.method == req.method.toString.toUpperCase)
      .map(_.mappings)
      .flatMap(routingTo(req, _))
      .orElse(strResponse(str = Constant.NOT_FOUND, status = HttpResponseStatus.NOT_FOUND))
  }

  private def routingTo(req: FullHttpRequest, mapping: Mappings): Option[DefaultFullHttpResponse] = {
    try {
      mapping.execute(req).flatMap {
        case str: String => strResponse(str)
        case bytes: Array[Byte] => response(allocator.heapBuffer(bytes.length).writeBytes(bytes), contentType = "application/octet-stream")
        case ref: AnyRef => Option(ref).map(JSON.toJSONString(_)).flatMap(strResponse(_, contentType = "application/json"))
        case _ => strResponse("null")
      }
    } catch {
      case e: Exception =>
        log.error("execute mapping error:\r\n", e)
        strResponse(str = Option(e.getMessage).getOrElse("exception with 417"), status = HttpResponseStatus.EXPECTATION_FAILED)
    }
  }

  private def response(i: ByteBuf, status: HttpResponseStatus = HttpResponseStatus.OK, contentType: String = "text/plain"): Option[DefaultFullHttpResponse] = {
    val response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, i)
    response.headers
      .set(HttpHeaderNames.CONTENT_LENGTH, response.content.readableBytes)
      .set(HttpHeaderNames.CONTENT_TYPE, contentType)
    Option(response)
  }


  private def strResponse(str: String, charset: Charset = CharsetUtil.UTF_8, status: HttpResponseStatus = HttpResponseStatus.OK, contentType: String = "text/plain"): Option[DefaultFullHttpResponse] = {
    response(Unpooled.copiedBuffer(str, charset), status)
  }


}
