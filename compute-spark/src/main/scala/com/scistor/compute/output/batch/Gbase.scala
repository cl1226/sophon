package com.scistor.compute.output.batch

import java.util

import com.scistor.compute.apis.BaseOutput
import com.scistor.compute.model.remote.TransStepDTO
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConversions.mapAsScalaMap

class Gbase extends BaseOutput {

  var config: TransStepDTO = _

  /**
   * Set Config.
   **/
  override def setConfig(config: TransStepDTO): Unit = {
    this.config = config
  }

  /**
   * Get Config.
   **/
  override def getConfig(): TransStepDTO = config

  /**
   * Return true and empty string if config is valid, return false and error message if config is invalid.
   */
  override def validate(): (Boolean, String) = {
    val attrs = config.getStepAttributes
    if (!attrs.containsKey("connectUrl")) {
      (false, s"please specify [connectUrl] in ${attrs.getOrElse("dataSourceType", "")} as a non-empty string")
    } else {
      (true, "")
    }
  }

  override def process(df: Dataset[Row]): Unit = {
    val attrs = config.getStepAttributes
    println(s"[INFO] 输出数据源 [${config.getStepType}] properties: ")
    attrs.foreach(entry => {
      val (key, value) = entry
      println("\t" + key + " = " + value)
    })
    val definedProps = attrs.get("properties").asInstanceOf[util.Map[String, AnyRef]]
    val saveMode = definedProps.getOrDefault("saveMode", "append").toString

    val prop = new java.util.Properties
    prop.setProperty("driver", "com.gbase.jdbc.Driver")
    prop.setProperty("user", definedProps.get("user").toString)
    prop.setProperty("password", definedProps.get("password").toString)

    df.write.mode(saveMode).jdbc(attrs.get("connectUrl").toString, attrs.get("source").toString, prop)
  }
}
