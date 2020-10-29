package com.scistor.compute.input.batch

import com.scistor.compute.apis.BaseStaticInput
import com.scistor.compute.model.spark.SourceAttribute
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import scalaj.http
import scalaj.http.Http

class Http extends BaseStaticInput {

  var source: SourceAttribute = _

  /**
   * Set SourceAttribute.
   * */
  override def setSource(source: SourceAttribute): Unit = {
    this.source = source
  }

  /**
   * get SourceAttribute.
   * */
  override def getSource(): SourceAttribute = source

  /**
   * Return true and empty string if config is valid, return false and error message if config is invalid.
   */
  override def validate(): (Boolean, String) = {
    (true, "")
  }

  /**
   * Get DataFrame from this Static Input.
   * */
  override def getDataset(spark: SparkSession): Dataset[Row] = {
    import spark.implicits._

    val url = source.connection_url
    val response: http.HttpResponse[String] = Http(url).header("Accept", "application/json").timeout(10000, 1000).asString
    val result = response.body

    val ds = spark.createDataset(Seq(result))

    val format = source.decodeType.name().toLowerCase()
    val reader = spark.read.format(format)

    format match {
      case "json" => reader.option("mode", "PERMISSIVE").json(ds)
      case _ => ds.toDF()
    }

  }
}
