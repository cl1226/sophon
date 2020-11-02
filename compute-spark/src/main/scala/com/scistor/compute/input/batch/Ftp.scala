package com.scistor.compute.input.batch

import com.scistor.compute.apis.BaseStaticInput
import com.scistor.compute.model.remote.TransStepDTO
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class Ftp extends BaseStaticInput {

  var config: TransStepDTO = _

  /**
   * Set Config.
   * */
  override def setConfig(config: TransStepDTO): Unit = {
    this.config = config
  }

  /**
   * Get Config.
   * */
  override def getConfig(): TransStepDTO = config

  /**
   * Return true and empty string if config is valid, return false and error message if config is invalid.
   */
  override def validate(): (Boolean, String) = {
    val attrs = config.getStepAttributes
    attrs.containsKey("url") match {
      case true => {
        (true, "")
      }
      case false => (false, "please specify [url] as a non-empty string")
    }
    attrs.containsKey("format") match {
      case true => {
        (true, "")
      }
      case false => (false, "please specify [format] as a of [text, json, csv]")
    }
  }

  /**
   * Get DataFrame from this Static Input.
   * */
  override def getDataset(spark: SparkSession): Dataset[Row] = {
    val attrs = config.getStepAttributes
    val path = buildPathWithDefaultSchema(attrs.get("url").toString, "file://")
//    val path = buildPathWithDefaultSchema("ftp://ftpuser:123456@192.168.31.219:21/home/ftpuser/abc/param.csv", "file://")
    fileReader(spark, path)
  }

  protected def buildPathWithDefaultSchema(uri: String, defaultUriSchema: String): String = {

    val path = uri.startsWith("/") match {
      case true => defaultUriSchema + uri
      case false => uri
    }
    path
  }

  protected def fileReader(spark: SparkSession, path: String): Dataset[Row] = {
    val attrs = config.getStepAttributes
    import spark.implicits._

    val value = spark.sparkContext.wholeTextFiles(path)

    val format = attrs.get("format").toString.toLowerCase()
    val reader = spark.read.format(format)

    format match {
      case "text" => reader.load(path).withColumnRenamed("value", "raw_message")
      case "parquet" => reader.parquet(path)
      case "json" => {
        val x = value.flatMap(s => s._2.split("\n"))
        reader.option("mode", "PERMISSIVE").json(x)
      }
      case "orc" => reader.orc(path)
      case "csv" => {
        val x = value.flatMap(s => s._2.split("\r\n"))
        reader.option("header", "true").csv(x.toDS())
      }
      case _ => value.toDF()
    }

  }
}
