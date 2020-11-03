package com.scistor.compute.input.batch

import com.scistor.compute.apis.BaseStaticInput
import com.scistor.compute.model.remote.TransStepDTO
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class File extends BaseStaticInput {

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
    attrs.containsKey("catalog") match {
      case true => {
        // TODO CHECK hosts
        (true, "")
      }
      case false => (false, "please specify [catalog] as a non-empty string")
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
   **/
  override def getDataset(spark: SparkSession): Dataset[Row] = {
    val attrs = config.getStepAttributes
    val path = buildPathWithDefaultSchema(attrs.get("catalog").toString, "file://")
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
    val format = attrs.get("format").toString.toLowerCase()
    val reader = spark.read.format(format)

    format match {
      case "text" => reader.load(path).withColumnRenamed("value", "raw_message")
      case "parquet" => reader.parquet(path)
      case "json" => reader.option("mode", "PERMISSIVE").json(path)
      case "orc" => reader.orc(path)
      case "csv" => {
        var delimiter: String = ","
        if(attrs.containsKey("delimiter") && StringUtils.isNotBlank(attrs.get("delimiter").toString)) {
          delimiter = attrs.get("delimiter").toString
        }
        reader.option("header", "true").option("delimiter", delimiter).csv(path)
      }
      case _ => reader.format(format).load(path)
    }
  }
}
