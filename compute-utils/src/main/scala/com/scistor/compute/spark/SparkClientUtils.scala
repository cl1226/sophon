package com.scistor.compute.spark

import org.apache.spark.launcher.{SparkAppHandle, SparkLauncher}

import java.util.Properties

class SparkClientUtils {

  private val sparkCommand = new SparkCommand

  def this(properties: Properties) {
    this()
    this.sparkCommand.init(properties)
  }

  def build(jobName: String): Unit = {

  }

  def submit(jobName: String): Unit = {

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
      .setVerbose(true).startApplication(new SparkAppHandle.Listener {
      override def stateChanged(sparkAppHandle: SparkAppHandle): Unit = {
        println("state:" + sparkAppHandle.getState().toString())
      }

      override def infoChanged(sparkAppHandle: SparkAppHandle): Unit = {
        println("Info:" + sparkAppHandle.getState().toString())
      }
    })

    while (!"FINISHED".equalsIgnoreCase(handler.getState.toString)
      && !"FAILED".equalsIgnoreCase(handler.getState.toString)) {
      println(s"id: ${handler.getAppId}")
      println(s"state: ${handler.getState.toString}")
      Thread.sleep(1000)
    }
  }
}

object SparkClientUtils {

  def main(args: Array[String]): Unit = {
    val properties = new Properties
    properties.load(this.getClass.getResourceAsStream("/spark_cmd.properties"))
//    val sparkUtils = new SparkClientUtils(properties)
//    sparkUtils.submit("test_job")

    val boot = new LauncherBoot(properties)
    boot.waitForCompleted()
  }

}
