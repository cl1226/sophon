package com.scistor.compute.output.batch

import java.util
import java.util.Properties

import com.scistor.compute.apis.BaseOutput
import com.scistor.compute.model.remote.TransStepDTO
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConversions._

class Jdbc extends BaseOutput {

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
    (true, "")
  }

  override def process(df: Dataset[Row]): Unit = {
    val attrs = config.getStepAttributes
    println(s"[INFO] 输出数据源 [${config.getStepType}] properties: ")
    attrs.foreach(entry => {
      val (key, value) = entry
      println("\t" + key + " = " + value)
    })
    val prop = new Properties()
    val definedProps = attrs.get("properties").asInstanceOf[util.Map[String, AnyRef]]
    for ((k, v) <- definedProps) {
      prop.setProperty(k, v.toString)
    }
    prop.setProperty("driver", "com.mysql.cj.jdbc.Driver")

    val saveMode = definedProps.getOrElse("saveMode", "append").toString

    saveMode match {
      case "overwrite" => {
        df.write.mode(saveMode)
          .option("truncate", "true")
          .option("batchsize", "50000")
          .jdbc(attrs.get("connectUrl").toString, attrs.get("tableName").toString, prop)
      }
      case _ => df.write.mode(saveMode)
        .option("batchsize", "50000")
        .jdbc(attrs.get("connectUrl").toString, attrs.get("tableName").toString, prop)
    }


  }
}
