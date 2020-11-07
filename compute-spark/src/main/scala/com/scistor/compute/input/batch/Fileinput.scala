package com.scistor.compute.input.batch

import com.scistor.compute.apis.BaseStaticInput
import com.scistor.compute.model.remote.TransStepDTO
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ComputeDataType, DataTypes, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import scalaj.http
import scalaj.http.Http

import scala.collection.JavaConversions._
import scala.collection.mutable

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

    println(s"[INFO] 输入数据源 <${config.getStepType}> properties: ")
    attrs.foreach(entry => {
      val (key, value) = entry
      println("\t" + key + " = " + value)
    })

    val response: http.HttpResponse[String] = Http(attrs.get("fileUrl").toString).header("Accept", "application/json").timeout(10000, 1000).asString
    val result = response.body

    val format = attrs.get("fileType").toString.toLowerCase()
    val reader = spark.read.format(format)

    format match {
      case "txt" => {
        val dataType = ComputeDataType.fromStructField(config.getOutputFields.get(0).getFieldType.toLowerCase())
        val streamFieldName = config.getOutputFields.get(0).getStreamFieldName
        var df = spark.createDataset(result.split("\n").toSeq).toDF(streamFieldName)
        df = df.withColumn(streamFieldName, col(streamFieldName).cast(dataType))
        df
      }
      case "json" => {
        val ds = spark.createDataset(result.split("\n").toSeq)
        var df = reader.option("mode", "PERMISSIVE").json(ds)
        config.getOutputFields.foreach(output => {
          val dataType = ComputeDataType.fromStructField(output.getFieldType.toLowerCase())
          df = df.withColumn(output.getStreamFieldName, col(output.getStreamFieldName).cast(dataType))
        })
        df
      }
      case "csv" => {
        val ds = spark.createDataset(result.split("\r\n"))
        var delimiter: String = ","
        if(attrs.containsKey("separator") && StringUtils.isNotBlank(attrs.get("separator").toString)) {
          delimiter = attrs.get("separator").toString
        }
        if (attrs.containsKey("header") && attrs.get("header").toString.equals("true")) {
          reader.option("header", true).option("delimiter", delimiter).csv(ds)
        } else {
          val frame = reader.option("delimiter", delimiter).csv(ds)
          val structFields = mutable.ArrayBuffer[StructField]()
          config.getOutputFields.foreach(output => {
            structFields += DataTypes.createStructField(output.getStreamFieldName, ComputeDataType.fromStructField(output.getFieldType), true)
          })
          val structType = DataTypes.createStructType(structFields.toArray)
          spark.createDataFrame(frame.rdd, structType)
        }
      }
      case _ => spark.createDataset(result.split("\n").toSeq).toDF()
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
