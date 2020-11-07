package com.scistor.compute.input.batch

import com.scistor.compute.apis.BaseStaticInput
import com.scistor.compute.model.remote.TransStepDTO
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ComputeDataType, DataTypes, StructField}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._
import scala.collection.mutable

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

    println(s"[INFO] 输入数据源 <${config.getStepType}> properties: ")
    attrs.foreach(entry => {
      val (key, value) = entry
      println("\t" + key + " = " + value)
    })

    val format = attrs.get("format").toString.toLowerCase()
    val reader = spark.read.format(format)

    format match {
      case "text" => reader.load(path).withColumnRenamed("value", "raw_message")
      case "parquet" => reader.parquet(path)
      case "json" => {
        var df = reader.option("mode", "PERMISSIVE").json(path)
        config.getOutputFields.foreach(output => {
          val dataType = ComputeDataType.fromStructField(output.getFieldType.toLowerCase())
          df = df.withColumn(output.getStreamFieldName, col(output.getStreamFieldName).cast(dataType))
        })
        df
      }
      case "orc" => reader.orc(path)
      case "csv" => {
        var delimiter: String = ","
        if(attrs.containsKey("separator") && StringUtils.isNotBlank(attrs.get("separator").toString)) {
          delimiter = attrs.get("separator").toString
        }
        if (attrs.containsKey("header") && attrs.get("header").toString.equals("true")) {
          reader.option("header", true).option("delimiter", delimiter).csv(path)
        } else {
          val frame = reader.option("delimiter", delimiter).csv(path)
          val structFields = mutable.ArrayBuffer[StructField]()
          config.getOutputFields.foreach(output => {
            structFields += DataTypes.createStructField(output.getStreamFieldName, ComputeDataType.fromStructField(output.getFieldType), true)
          })
          val structType = DataTypes.createStructType(structFields.toArray)
          spark.createDataFrame(frame.rdd, structType)
        }
      }
      case _ => reader.format(format).load(path)
    }
  }
}
