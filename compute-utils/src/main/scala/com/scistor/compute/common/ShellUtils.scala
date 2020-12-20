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

  def runShell(command: String, prefix: String): Unit = {
    val cmd: String = prefix match {
      case "" => command
      case _ => s"$prefix && $command"
    }
    cmd.indexOf("ssh") >= 0 match {
      case true => Process(cmd).run(new ComputeProcessLogger)
      case false => {
        val cmd2 = Array("/bin/sh", "-c", cmd)
        Runtime.getRuntime.exec(cmd2)
      }
    }
  }

  def runShellBlock(command: String, prefix: String): String = {
    val cmd: String = prefix match {
      case "" => command
      case _ => s"$prefix $command"
    }
    cmd.indexOf("ssh") >= 0 match {
      case true => Process(cmd).!!(new ComputeProcessLogger)
      case false => {
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
        println(s"result: ${stringBuffer.toString}")
        stringBuffer.toString
      }
    }
  }
}
