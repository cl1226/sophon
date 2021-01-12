package com.scistor.compute.utils

import com.alibaba.fastjson.JSON

import java.io.{BufferedReader, InputStreamReader, OutputStream}
import java.net.{HttpURLConnection, URL}
import java.nio.charset.Charset
import java.util.stream.Collectors

object HttpUtils {

  def doPost(url: String, param: String): String =  {
    val u = new URL(url)
    val conn: HttpURLConnection = u.openConnection().asInstanceOf[HttpURLConnection]
    conn.setDoOutput(true)
    conn.setDoInput(true)
    conn.setUseCaches(false)
    conn.setRequestProperty("Content-Type", "application/json;charset=utf-8")
    conn.setRequestMethod("POST")
    conn.connect()

    val outputStream: OutputStream = conn.getOutputStream
    outputStream.write(param.getBytes)
    outputStream.flush()
    outputStream.close()

    val inputStream = conn.getInputStream
    val result: String = new BufferedReader(new InputStreamReader(inputStream)).lines().collect(Collectors.joining(System.lineSeparator()))

//    val data = new Array[Byte](1024)
//    val res = new StringBuffer
//    var length = 0
//    while ((length = inputStream.read(data)) != -1) {
//      val s = new String(data, Charset.forName("utf-8"))
//      res.append(s)
//    }
//    val result = JSON.parse(res.toString).toString
    inputStream.close()
    conn.disconnect()
    result
  }

}
