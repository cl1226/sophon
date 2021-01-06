package com.scistor.compute.spark

import com.scistor.compute.common.ExecuteShellUtil
import org.apache.spark.launcher.{SparkAppHandle, SparkLauncher}

import java.text.SimpleDateFormat
import java.util.{Date, Properties}
import scala.sys.process.Process

class SparkClientUtils {

  private val sparkCommand = new SparkCommand

  def this(properties: Properties) {
    this()
    this.sparkCommand.init(properties)
  }

  def buildCommand(jobName: String): String = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val timestamp = format.format(new Date())
    val command =
      s"""
         |${sparkCommand.sparkHome}/bin/spark-submit
         | --queue ${sparkCommand.queue}
         | --master ${sparkCommand.master}
         | --deploy-mode ${sparkCommand.deployMode}
         | --name $jobName
         | --class ${sparkCommand.mainClass}
         | --conf 'spark.executor.memory=${sparkCommand.executorMemory}'
         | --conf 'spark.driver.memory=${sparkCommand.driverMemory}'
         | --conf 'spark.executor.cores=${sparkCommand.executorCores}'
         | --conf 'spark.executor.instances=${sparkCommand.executorInstances}'
         | --conf 'spark.yarn.maxAppAttempts=${sparkCommand.maxAppAttempts}'
         | --conf 'spark.yarn.submit.waitAppCompletion=${sparkCommand.waitAppCompletion}'
         | --jars ${sparkCommand.extraJars}
         | > ${sparkCommand.logDir}/compute-spark-${jobName}-${timestamp}.log 2>&1
         |""".stripMargin.replaceAll("\n", " ").trim.replaceAll("\r", "")
    command
  }

  def submit(jobName: String): Unit = {
//    val command = this.buildCommand(jobName)
//    if (command.indexOf("ssh") >= 0) {
//      Process(command).run()
//    } else {
//      val cmd = Array("/bin/sh", "-c", command)
//      Runtime.getRuntime.exec(cmd)
//    }

    val host = sparkCommand.sparkClientHost
    val port = sparkCommand.sparkClientPort
    val username = sparkCommand.sparkClientUser
    val password = sparkCommand.sparkClientPwd
    val executeShellUtil = ExecuteShellUtil.getInstance
    executeShellUtil.init(host, Integer.valueOf(port), username, password)

    val handler: SparkAppHandle = new SparkLauncher()
      .setAppResource(sparkCommand.appResource)
      .setMainClass(sparkCommand.mainClass)
      .setAppName(jobName)
      .setDeployMode(sparkCommand.deployMode)
      .setMaster(sparkCommand.master)
      .setSparkHome(sparkCommand.sparkHome)
      .addAppArgs(sparkCommand.args)
      .setConf("spark.queue", sparkCommand.queue)
      .setConf("spark.driver.memory", sparkCommand.driverMemory)
      .setConf("spark.executor.memory", sparkCommand.executorMemory)
      .setConf("spark.executor.cores", sparkCommand.executorCores)
      .setConf("spark.executor.instance", sparkCommand.executorInstances)
      .setConf("spark.yarn.maxAppAttempts", sparkCommand.maxAppAttempts)
      .setConf("spark.yarn.submit.waitAppCompletion", sparkCommand.waitAppCompletion)
      .setVerbose(true).startApplication()

//    while (!"FINISHED".equalsIgnoreCase(handler.getState.toString)
//      && !"FAILED".equalsIgnoreCase(handler.getState.toString)) {
//      println(s"id: ${handler.getAppId}")
//      println(s"state: ${handler.getState.toString}")
//      Thread.sleep(1000)
//    }
  }
}

object SparkClientUtils {

  def main(args: Array[String]): Unit = {
    val properties = new Properties
    properties.load(this.getClass.getResourceAsStream("/spark_cmd.properties"))
    val sparkUtils = new SparkClientUtils(properties)
    sparkUtils.submit("test_job")

//    val boot = new LauncherBoot(properties)
//    boot.waitForCompleted()
  }

}
