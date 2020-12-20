package com.scistor.compute.common

import java.util.Properties

object PropertyUtils {

  var yarnRM1: String = _
  var yarnRM2: String = _

  var sparkHome: String = _
  var driverMemory: String = _
  var executorMemory: String = _
  var executorCores: String = _
  var sparkDiyJar: String = _

  var prefixCommand: String = _
  var krb5Principal: String = _
  var krb5KeyTab: String = _
  var krb5Config: String = _

  var clientHost: String = _
  var clientUser: String = _
  var clientPort: String = _

  var kafkaJaas: String = _
  var huaweiJaas: String = _

  def loadProperties(prop: Properties): Unit = {
    yarnRM1 = prop.getProperty("yarn.rm1")
    yarnRM2 = prop.getProperty("yarn.rm2")
    sparkHome = prop.getProperty("spark.home")
    driverMemory = prop.getProperty("driver.memory")
    executorMemory = prop.getProperty("executor.memory")
    executorCores = prop.getProperty("executor.cores")
    sparkDiyJar = prop.getProperty("spark.diyjar.path")
    prefixCommand = prop.getProperty("prefix.command")
    krb5Principal = prop.getProperty("krb5.principal")
    krb5KeyTab = prop.getProperty("krb5.keytab")
    krb5Config = prop.getProperty("krb5.config")
    clientHost = prop.getProperty("client.host")
    clientUser = prop.getProperty("client.user")
    clientPort = prop.getProperty("client.port")
    kafkaJaas = prop.getProperty("kafka.jaas")
    huaweiJaas = prop.getProperty("huawei.jaas")
  }

}
