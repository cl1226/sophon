package com.scistor.compute.input.batch

import com.scistor.compute.apis.BaseStaticInput
import com.scistor.compute.model.spark.SourceAttribute
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class Ftp extends BaseStaticInput {

  var sourceAttribute: SourceAttribute = _

  /**
   * Set SourceAttribute.
   * */
  override def setSource(source: SourceAttribute): Unit = {
    this.sourceAttribute = source
  }

  /**
   * get SourceAttribute.
   * */
  override def getSource(): SourceAttribute = sourceAttribute

  /**
   * Get DataFrame from this Static Input.
   * */
  override def getDataset(spark: SparkSession): Dataset[Row] = {
    val path = buildPathWithDefaultSchema(sourceAttribute.connection_url, "file://")
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

    import spark.implicits._

    val value = spark.sparkContext.wholeTextFiles(path)

    val format = sourceAttribute.decodeType.name().toLowerCase()
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
