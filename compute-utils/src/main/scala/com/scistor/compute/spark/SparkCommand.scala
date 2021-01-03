package com.scistor.compute.spark

import java.util.Properties

class SparkCommand {

  var appResource: String = _
  var appName: String = _
  var mainClass: String = _
  var logDir: String = _

  var yarnRM1: String = _
  var yarnRM2: String = _

  var krbEnable: String = _
  var principal: String = _
  var keytab: String = _
  var krb5Conf: String = _

  var sparkHome: String = _
  var extraJars: String = _
  var driverMemory: String = _
  var executorMemory: String = _
  var executorCores: String = _
  var executorInstances: String = _
  var master: String = _
  var deployMode: String = _
  var queue: String = _
  var args: String = _
  var maxAppAttempts: String = _
  var waitAppCompletion: String = _

  var sparkClientHost: String = _
  var sparkClientUser: String = _
  var sparkClientPort: String = _

  var kafkaJaasConf: String = _
  var javaJaasConf: String = _

  def init(properties: Properties): Unit = {
    this.appResource = properties.getProperty("appResource")
    this.appName = properties.getProperty("appName")
    this.mainClass = properties.getProperty("mainClass")
    this.logDir = properties.getProperty("log.dir")
    this.yarnRM1 = properties.getProperty("yarn.rm1")
    this.yarnRM2 = properties.getProperty("yarn.rm2")
    this.krbEnable = properties.getProperty("server.enabled")
    this.principal = properties.getProperty("spark.principal")
    this.keytab = properties.getProperty("spark.keyTab")
    this.krb5Conf = properties.getProperty("server.config")
    this.sparkHome = properties.getProperty("spark.home")
    this.extraJars = properties.getProperty("spark.diyjar.path")
    this.driverMemory = properties.getProperty("driver.memory")
    this.executorMemory = properties.getProperty("executor.memory")
    this.executorCores = properties.getProperty("executor.cores")
    this.executorInstances = properties.getProperty("executor.instances")
    this.sparkClientHost = properties.getProperty("remote.address.host")
    this.sparkClientUser = properties.getProperty("remote.address.user")
    this.sparkClientPort = properties.getProperty("remote.address.ssh.port")
    this.kafkaJaasConf = properties.getProperty("jaas.yarn.config")
    this.javaJaasConf = properties.getProperty("huawei.rest.jaas")
    this.master = properties.getProperty("master")
    this.deployMode = properties.getProperty("deployMode")
    this.args = properties.getProperty("args")
    this.queue = properties.getProperty("queue")
    this.maxAppAttempts = properties.getProperty("maxAppAttempts")
    this.waitAppCompletion = properties.getProperty("waitAppCompletion")
  }

}
