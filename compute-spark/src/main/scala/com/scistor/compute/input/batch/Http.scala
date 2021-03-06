package com.scistor.compute.input.batch

import java.util

import com.scistor.compute.apis.BaseStaticInput
import com.scistor.compute.model.remote.TransStepDTO
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.ComputeDataType
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import scalaj.http
import scalaj.http.Http

import scala.collection.JavaConversions._

class Http extends BaseStaticInput {

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
    val extraProps = attrs.get("properties").asInstanceOf[util.Map[String, AnyRef]]
    if (!attrs.containsKey("url")) {
      (false, s"please specify [url] in ${config.getStepType} as a non-empty string")
    } else {
      (true, "")
    }
  }

  /**
   * Get DataFrame from this Static Input.
   * */
  override def getDataset(spark: SparkSession): Dataset[Row] = {
    import spark.implicits._
    val attrs = config.getStepAttributes

    println(s"[INFO] 输入数据源 [${config.getStepType}] properties: ")
    attrs.foreach(entry => {
      val (key, value) = entry
      println("\t" + key + " = " + value)
    })

    val url = attrs.get("url").toString
    val response: http.HttpResponse[String] = Http(url).header("Accept", "application/json").timeout(10000, 1000).asString
    val result = response.body

    val ds = spark.createDataset(Seq(result))

    val format = attrs.get("dataFormatType").toString.toLowerCase()
    val reader = spark.read.format(format)

    format match {
      case "json" => {
        var df = reader.option("mode", "PERMISSIVE").json(ds)
        config.getOutputFields.foreach(output => {
          val dataType = ComputeDataType.fromStructField(output.getFieldType.toLowerCase())
          df = df.withColumn(output.getStreamFieldName, col(output.getStreamFieldName).cast(dataType))
        })
        df
      }
      case _ => ds.toDF()
    }

  }
}
