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
  var executorCores: Int = _

  var sparkClientHost: String = _
  var sparkClientUser: String = _
  var sparkClientPort: String = _

  var kafkaJaasConf: String = _
  var javaJaasConf: String = _

  def init(properties: Properties): Unit = {
    this.appResource = properties.getProperty("appResource")
    this.appName = properties.getProperty("appName")
    this.mainClass = properties.getProperty("mainClass")
  }

}
