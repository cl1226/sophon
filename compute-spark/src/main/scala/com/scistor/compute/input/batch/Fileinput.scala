package com.scistor.compute.input.batch

import com.scistor.compute.apis.BaseStaticInput
import com.scistor.compute.model.remote.TransStepDTO
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import scalaj.http
import scalaj.http.Http

class Fileinput extends BaseStaticInput {

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
   * Get DataFrame from this Static Input.
   * */
  override def getDataset(spark: SparkSession): Dataset[Row] = {
    import spark.implicits._

    val attrs = config.getStepAttributes
    val response: http.HttpResponse[String] = Http(attrs.get("fileUrl").toString).header("Accept", "application/json").timeout(10000, 1000).asString
    val result = response.body

    val format = attrs.get("fileType").toString.toLowerCase()
    val reader = spark.read.format(format)

    format match {
      case "text" => spark.createDataset(Seq(result)).toDF()
      case "json" => {
        val ds = spark.createDataset(Seq(result))
        reader.option("mode", "PERMISSIVE").json(ds)
      }
      case "csv" => {
        val ds = spark.createDataset(result.split("\r\n"))
        var delimiter: String = ","
        if(attrs.containsKey("separator") && StringUtils.isNotBlank(attrs.get("separator").toString)) {
          delimiter = attrs.get("separator").toString
        }
        reader.option("header", "true").option("delimiter", delimiter).csv(ds)
      }
      case _ => spark.createDataset(Seq(result)).toDF()
    }
  }

  /**
   * Return true and empty string if config is valid, return false and error message if config is invalid.
   */
  override def validate(): (Boolean, String) = {
    val attrs = config.getStepAttributes
    if(!attrs.containsKey("fileUrl") || attrs.get("fileUrl").toString.equals("")) {
      (false, "please special [FileInput] fileUrl as string")
    } else {
      (true, "")
    }
  }
}
