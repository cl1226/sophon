package com.scistor.compute.common

import java.io.InputStreamReader
import scala.sys.process._

class ComputeProcessLogger extends ProcessLogger {
  override def out(s: => String): Unit = {

  }

  override def err(s: => String): Unit = {

  }

  override def buffer[T](f: => T): T = f
}

object ShellUtils {

  def runShell(remoteConfig: RemoteConfig, command: String): Unit = {
    val command2 = s"ssh -p${remoteConfig.port} ${remoteConfig.user}@${remoteConfig.host} ${command}"
    Process(command2).run(new ComputeProcessLogger)
  }

  def runShellBlock(remoteConfig: RemoteConfig, command: String): String = {
//    val command2 = s"ssh -p${remoteConfig.port} ${remoteConfig.user}@${remoteConfig.host} ${command}"
//    val result: String = Process(command2).!!(new ComputeProcessLogger)
    val cmds = Array("/bin/sh", "-c", command)
    val process = Runtime.getRuntime.exec(cmds)
    process.waitFor()
    val inputStream = process.getInputStream
    val reader = new InputStreamReader(inputStream)
    val buffer = new Array[Char](1024)
    var bytes_read = reader.read(buffer)
    val stringBuffer = new StringBuffer()
    while (bytes_read > 0) {
      stringBuffer.append(buffer, 0, bytes_read)
      bytes_read = reader.read(buffer)
    }
    stringBuffer.toString
  }

  def runShellLineStream(remoteConfig: RemoteConfig, command: String): String = {
    val command2 = s"ssh -p${remoteConfig.port} ${remoteConfig.user}@${remoteConfig.host} ${command}"
    val result: Stream[String] = Process(command2).lineStream_!(new ComputeProcessLogger)
    result.head
  }

}
