package com.scistor.compute.http

import java.nio.charset.{Charset, StandardCharsets}

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import io.netty.buffer.{ByteBuf, ByteBufUtil}
import io.netty.handler.codec.http.{FullHttpRequest, HttpHeaders}

class MappingCtx(req: FullHttpRequest) {

  val charset: Charset = StandardCharsets.UTF_8

  def jsonArray(): Option[JSONArray] = jsonValue(true).map(_.asInstanceOf[JSONArray])

  def jsonObject(): Option[JSONObject] = jsonValue().map(_.asInstanceOf[JSONObject])

  def stringValue(charset: Charset = charset): Option[String] = convertByte2Str(req.content())

  def byteValue(): Array[Byte] = ByteBufUtil.getBytes(req.content())

  def header(name: String): String = req.headers().get(name)

  def headers(): HttpHeaders = req.headers()

  private def convertByte2Str(b: ByteBuf, charsets: Charset = charset): Option[String] = Option(b).map(b => b.toString(charsets))

  private def jsonValue(isArray: Boolean = false): Option[JSON] = Option(req.content())
    .flatMap(convertByte2Str(_))
    .filter(str => if (isArray) JSON.isValidArray(str) else JSON.isValidObject(str))
    .map(str => if (isArray) JSON.parseArray(str) else JSON.parseObject(str))

  def toObject[T](cls: Class[T]): Option[T] = stringValue().filter(JSON.isValidObject).map(JSON.parseObject(_, cls))

}
