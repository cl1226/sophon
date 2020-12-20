package com.scistor.compute.output.batch

import java.text.{DateFormat, SimpleDateFormat}
import java.util.Calendar
import java.util

import com.scistor.compute.apis.BaseOutput
import com.scistor.compute.model.remote.TransStepDTO
import org.apache.spark.sql.{DataFrameWriter, Dataset, Row, SaveMode}

import scala.collection.JavaConversions._

abstract class FileOutputBase extends BaseOutput {

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

  protected def fileWriter(df: Dataset[Row]): DataFrameWriter[Row] = {
    val attrs = config.getStepAttributes
    df.write.format(attrs.get("dataFormatType").toString.toLowerCase())
  }

  protected def buildPathWithDefaultSchema(uri: String, defaultUriSchema: String): String = {

    val path = uri.startsWith("/") match {
      case true => defaultUriSchema + uri
      case false => uri
    }

    path
  }

  def processImpl(df: Dataset[Row], defaultUriSchema: String): Unit = {
    val attrs = config.getStepAttributes
    println(s"[INFO] 输出数据源 [${config.getStepType}] properties: ")
    attrs.foreach(entry => {
      val (key, value) = entry
      println("\t" + key + " = " + value)
    })
    val writer = fileWriter(df)
    var path = buildPathWithDefaultSchema(attrs.get("connectAddress").toString, defaultUriSchema)
    val writeProps = attrs.get("write").asInstanceOf[util.Map[String, AnyRef]]
    val hdfsRange = writeProps.getOrDefault("catalogPattern", "yyyyMMdd/timestamp/*").toString
    var dateFormat: DateFormat = null
    val calendar = Calendar.getInstance()
    val res = hdfsRange.split("/").length match {
      case 2 => {
        val SP = "((.*)/(.*))".r
        val SP(all, a, b) = hdfsRange.replace("*", "")
        dateFormat = new SimpleDateFormat(a)
        val now = dateFormat.format(calendar.getTime)
        b match {
          case "" => {
            val date = dateFormat.format(calendar.getTime)
            hdfsRange.replace(a, date).replace("*", "")
          }
          case _ => {
            calendar.set(Calendar.MINUTE, 0)
            calendar.set(Calendar.SECOND, 0)
            calendar.set(Calendar.MILLISECOND, 0)
            val timestamp = calendar.getTime.getTime.toString
            hdfsRange.replace(a, now).replace(b, timestamp).replace("*", "")
          }
        }
      }
      case 3 => {
        val SP = "((.*)/(.*)/(.*))".r
        val SP(all, a, b, c) = hdfsRange.replace("*", "")
        dateFormat = new SimpleDateFormat(a)
        val now = dateFormat.format(calendar.getTime)
        calendar.set(Calendar.MINUTE, 0)
        calendar.set(Calendar.SECOND, 0)
        calendar.set(Calendar.MILLISECOND, 0)
        val timestamp = calendar.getTime.getTime.toString
        hdfsRange.replace(a, now).replace(b, timestamp).replace("*", "")
      }
    }

    path = path + "/" + res

    val saveMode = writeProps.getOrDefault("saveMode", "append").toString

    val format = attrs.get("dataFormatType").toString.toLowerCase()
    format match {
      case "csv" => writer.option("delimiter", attrs.getOrDefault("separator", ",").toString).mode(saveMode).csv(path)
      case "json" => writer.mode(saveMode).json(path)
      case "parquet" => writer.mode(saveMode).parquet(path)
      case "text" => writer.text(path)
      case "orc" => writer.orc(path)
      case _ => writer.format(format).save(path)
    }
  }
}
