package com.scistor.compute.common

import scala.sys.process._
import scala.util.{Failure, Success, Try}

class ComputeProcessLogger extends ProcessLogger {
  override def out(s: => String): Unit = {
  }

  override def err(s: => String): Unit = {
  }

  override def buffer[T](f: => T): T = f
}

object ShellUtils {

  var debug: Boolean = false

  def runShell(command: String, prefix: String): Unit = {
    val cmd: String = prefix match {
      case "" => command
      case _ => s"$prefix $command"
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
    if (debug) {
      println(s"cmd: $cmd")
    }
    var res: String = ""
    cmd.indexOf("ssh") >= 0 match {
      case true => {
        Try{res = Process(cmd).!!(new ComputeProcessLogger)} match {
          case Success(value) => res
          case Failure(exception) => ""
        }

      }
      case false => {
        val cmds = Array("/bin/sh", "-c", cmd)
        val process = Runtime.getRuntime.exec(cmds)
        val sb = new StringBuffer
        val sb2 = new StringBuffer
        val inputStream = process.getInputStream
        val errorStream = process.getErrorStream

        val thread1=new Thread(new RunThread(inputStream, sb))
        thread1.start()
        val thread2=new Thread(new RunThread(errorStream, sb2))
        thread2.start()

        process.waitFor()

        thread1.interrupt()
        thread2.interrupt()

        sb.toString()
      }
    }
  }
}
