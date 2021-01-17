package com.scistor.compute.common

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.util.function.Consumer
import scala.util.control.Breaks.{break, breakable}

class RunThread extends Runnable {

  var inputStream: InputStream = _
//  var consumer: Consumer[String] = _
  var sb:StringBuffer = _

  def this(inputStream: InputStream, sb: StringBuffer) {
    this()
    this.inputStream = inputStream
    this.sb = sb
  }

  override def run(): Unit = {
    val stream=new BufferedReader(new InputStreamReader(inputStream))
    var line: String = ""
    breakable {
      while ((line = stream.readLine()) != null){
        if (null == line) break
        sb.append(line)
        sb.append("\n")
      }
    }

  }
}
