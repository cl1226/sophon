package com.scistor.compute.output.batch

import java.text.SimpleDateFormat
import java.util.Calendar

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
    df.write.mode(attrs.get("format").toString.toLowerCase())
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
    println(s"[INFO] 输出数据源 <${config.getStepType}> properties: ")
    attrs.foreach(entry => {
      val (key, value) = entry
      println("\t" + key + " = " + value)
    })
    val writer = fileWriter(df)
    var path = buildPathWithDefaultSchema(attrs.get("connectAddress").toString, defaultUriSchema)
    val format = attrs.get("format").toString.toLowerCase()
    format match {
      case "csv" => writer.option("delimiter", attrs.get("split").toString).mode(saveMode = SaveMode.Append).csv(path)
      case "json" =>
        val calendar = Calendar.getInstance()
        val format = new SimpleDateFormat("yyyy-MM-dd")
        calendar.add(Calendar.HOUR, -1)
        calendar.set(Calendar.MINUTE, 0)
        calendar.set(Calendar.SECOND, 0)
        calendar.set(Calendar.MILLISECOND, 0)
        val timestamp = calendar.getTime.getTime
        val date = format.format(calendar.getTime)
        path += "/" + date + "/" + timestamp
        writer.mode(saveMode = SaveMode.Append).json(path)
      case "parquet" => writer.mode(saveMode = SaveMode.Append).parquet(path)
      case "text" => writer.text(path)
      case "orc" => writer.orc(path)
      case _ => writer.format(format).save(path)
    }
  }
}
