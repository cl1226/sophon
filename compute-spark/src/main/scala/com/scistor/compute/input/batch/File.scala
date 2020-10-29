package com.scistor.compute.input.batch

import com.scistor.compute.apis.BaseStaticInput
import com.scistor.compute.model.spark.SourceAttribute
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class File extends BaseStaticInput {

  var sourceAttribute: SourceAttribute = _

  /**
   * Set SourceAttribute.
   **/
  override def setSource(source: SourceAttribute): Unit = {
    this.sourceAttribute = source
  }

  /**
   * get SourceAttribute.
   **/
  override def getSource(): SourceAttribute = {
    this.sourceAttribute
  }


  /**
   * Return true and empty string if config is valid, return false and error message if config is invalid.
   */
  override def validate(): (Boolean, String) = {
    (true, "")
  }

  /**
   * Get DataFrame from this Static Input.
   **/
  override def getDataset(spark: SparkSession): Dataset[Row] = {
    val path = buildPathWithDefaultSchema(sourceAttribute.connection_url, "file://")
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
    val format = sourceAttribute.decodeType.name().toLowerCase()
    val reader = spark.read.format(format)

    format match {
      case "text" => reader.load(path).withColumnRenamed("value", "raw_message")
      case "parquet" => reader.parquet(path)
      case "json" => reader.option("mode", "PERMISSIVE").json(path)
      case "orc" => reader.orc(path)
      case "csv" => reader.csv(path)
      case _ => reader.format(format).load(path)
    }
  }
}
