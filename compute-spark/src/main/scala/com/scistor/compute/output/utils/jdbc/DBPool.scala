package com.scistor.compute.output.utils.jdbc

import java.sql.Connection
import java.util.Properties

import com.mchange.v2.c3p0.ComboPooledDataSource

class DBPool(properties: Properties) extends Serializable {

  var prop: Properties = properties

  private val cpds: ComboPooledDataSource = new ComboPooledDataSource(true)

  try {
    cpds.setJdbcUrl(prop.getProperty("dialect"))
    cpds.setDriverClass(prop.getProperty("driver"))
    cpds.setUser(prop.getProperty("user"))
    cpds.setPassword(prop.getProperty("password"))
  } catch {
    case e: Exception => e.printStackTrace()
  }

  def getConnection: Connection = {
    try {
      cpds.getConnection()
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        null
    }
  }

  def close() = {
    try {
      cpds.close()
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
    }
  }

}
