package com.scistor.compute.input.batch

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.{Dataset, Row, SparkSession}

class Hdfs extends File {

  /**
   * Return true and empty string if config is valid, return false and error message if config is invalid.
   */
  override def validate(): (Boolean, String) = {
    val attrs = config.getStepAttributes
    attrs.containsKey("connectAddress") match {
      case true => {
        // TODO CHECK hosts
        (true, "")
      }
      case false => (false, "please specify [connectAddress] as a non-empty string")
    }
    attrs.containsKey("dataFormatType") match {
      case true => {
        (true, "")
      }
      case false => (false, "please specify [format] as a of [text, json, csv, parquet, orc]")
    }
  }

  /**
   * Get DataFrame from this Static Input.
   **/
  override def getDataset(spark: SparkSession): Dataset[Row] = {
    val strategy = config.getStrategy

    if (strategy != null) {
      println(s"[INFO] 任务 <${config.getName}> properties: ")
      println(s"\t执行模式=${strategy.getRunMode}")
      println(s"\t时间单位=${strategy.getTimeUnit}")
    }

    val attrs = config.getStepAttributes
    var path = buildPathWithDefaultSchema(attrs.get("connectAddress").toString, "hdfs://")

    strategy.getRunMode match {
      case "single" => fileReader(spark, path)
      case "cycle" => {
        val calendar = Calendar.getInstance()
        val format = new SimpleDateFormat("yyyy-MM-dd")
        calendar.add(Calendar.HOUR, -1)
        calendar.set(Calendar.MINUTE, 0)
        calendar.set(Calendar.SECOND, 0)
        calendar.set(Calendar.MILLISECOND, 0)
        val date = format.format(calendar.getTime)
        val timestamp = calendar.getTime.getTime
        strategy.getTimeUnit match {
          case "HOUR" => {
            path = path + "/" + date + "/" + timestamp + "*"
          }
          case "DAILY" => {
            calendar.add(Calendar.DATE, -1)
            val date = format.format(calendar.getTime)
            path = path + "/" + date + "/*"
          }
        }
        fileReader(spark, path)
      }
    }
  }
}
