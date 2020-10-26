package com.scistor.compute.utils

import java.lang.reflect.Method

import org.apache.spark.internal.Logging
import org.apache.spark.sql.types._

import scala.collection.mutable

object UdfUtils extends Logging {

  lazy val jarPathMap = mutable.HashMap.empty[String, (Any, Method)]

  def getObjectAndMethod(udfName: String, className: String): (Any, Method) = {
    if (!jarPathMap.contains(udfName)) {
      val clazz = Class.forName(className)
      val o: Any = clazz.newInstance()
      val methods = clazz.getMethods
      var callMethod: Method = null
      for (i <- methods.indices) {
        val m: Method = methods(i)
        if (m.getName.equals(udfName)) {
          callMethod = m
        }
      }
      logInfo("reflect getMethod")
      jarPathMap(udfName) = (o, callMethod)
    }
    jarPathMap(udfName)
  }

  def convertSparkType(returnClassName: String) = {
    logInfo("convertSparkType:"+returnClassName)
    returnClassName match {
      case "int" => IntegerType
      case "java.lang.Integer" => IntegerType
      case "long" => LongType
      case "java.lang.Long" => LongType
      case "float" => FloatType
      case "java.lang.Float" => FloatType
      case "double" => DoubleType
      case "java.lang.Double" => DoubleType
      case "boolean" => BooleanType
      case "java.lang.Boolean" => BooleanType
      case "java.lang.String" => StringType
      case "java.math.BigDecimal" => DecimalType.SYSTEM_DEFAULT
      case "java.util.Date" => DateType
      case "java.sql.Date" => DateType
      case "java.sql.Timestamp" => TimestampType
      case "java.security.Timestamp" => TimestampType
      case "com.sun.jmx.snmp.Timestamp" => TimestampType
      case _ => BinaryType
    }
  }

  def removeUdf(udfName: String): Unit = {
    jarPathMap.remove(udfName)
  }

}
