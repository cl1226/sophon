package com.scistor.compute.input.batch

import java.text.{DateFormat, SimpleDateFormat}
import java.util
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
    val path = buildPathWithDefaultSchema(attrs.get("connectAddress").toString, "hdfs://")

    // 读取方式：全量(full)/增量(increment)
    val read = attrs.get("read").asInstanceOf[util.Map[String, AnyRef]]
    read.getOrDefault("type", "full").toString match {
      case "full" => fileReader(spark, path)
      case "increment" => {
        val calendar = Calendar.getInstance()
        val hdfsRange: String = read.getOrDefault("hdfsDirectoryTemplate", "yyyy-MM-dd/timestamp*").toString
        var dateFormat: DateFormat = null
        val hdfsTimeRange: Int = read.getOrDefault("hdfsTimeRange", "1").toString.toInt
        val res = hdfsRange.split("/").length match {
          case 2 => {
            val SP = "((.*)/(.*))".r
            val SP(all, a, b) = hdfsRange.replace("*", "")
            dateFormat = new SimpleDateFormat(a)
            val now = dateFormat.format(calendar.getTime)
            b match {
              case "" => {
                calendar.add(Calendar.DATE, -hdfsTimeRange)
                val date = dateFormat.format(calendar.getTime)
                hdfsRange.replace(a, date)
              }
              case _ => {
                calendar.add(Calendar.HOUR, -hdfsTimeRange)
                calendar.set(Calendar.MINUTE, 0)
                calendar.set(Calendar.SECOND, 0)
                calendar.set(Calendar.MILLISECOND, 0)
                val timestamp = calendar.getTime.getTime.toString
                hdfsRange.replace(a, now).replace(b, timestamp)
              }
            }
          }
          case 3 => {
            val SP = "((.*)/(.*)/(.*))".r
            val SP(all, a, b, c) = hdfsRange.replace("*", "")
            dateFormat = new SimpleDateFormat(a)
            val now = dateFormat.format(calendar.getTime)
            calendar.add(Calendar.HOUR, -hdfsTimeRange)
            calendar.set(Calendar.MINUTE, 0)
            calendar.set(Calendar.SECOND, 0)
            calendar.set(Calendar.MILLISECOND, 0)
            val timestamp = calendar.getTime.getTime.toString
            hdfsRange.replace(a, now).replace(b, timestamp)
          }
        }
        fileReader(spark, path + "/" + res)
      }
    }
  }
}
