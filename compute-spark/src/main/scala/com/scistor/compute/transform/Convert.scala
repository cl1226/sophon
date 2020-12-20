package com.scistor.compute.transform

import com.scistor.compute.apis.BaseTransform
import com.scistor.compute.model.remote.TransStepDTO
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{BooleanType, DoubleType, FloatType, IntegerType, LongType, StringType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions.mapAsScalaMap

class Convert extends BaseTransform {

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

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {
    val attrs = config.getStepAttributes

    println(s"[INFO] 转换算子 [${config.getStepType}] properties: ")
    attrs.foreach(entry => {
      val (key, value) = entry
      println("\t" + key + " = " + value)
    })

    val srcField = attrs.get("sourceField").toString
    val newType = attrs.get("newType").toString

    newType match {
      case "string" => df.withColumn(srcField, col(srcField).cast(StringType))
      case "integer" => df.withColumn(srcField, col(srcField).cast(IntegerType))
      case "double" => df.withColumn(srcField, col(srcField).cast(DoubleType))
      case "float" => df.withColumn(srcField, col(srcField).cast(FloatType))
      case "long" => df.withColumn(srcField, col(srcField).cast(LongType))
      case "boolean" => df.withColumn(srcField, col(srcField).cast(BooleanType))
      case _: String => df
    }
  }

  /**
   * Return true and empty string if config is valid, return false and error message if config is invalid.
   */
  override def validate(): (Boolean, String) = {
    val attrs = config.getStepAttributes
    if (!attrs.containsKey("sourceField")) {
      (false, "please specify [sourceType] as a non-empty string")
    } else if (!attrs.containsKey("newType")) {
      (false, "please specify [newType] as a non-empty string")
    } else {
      (true, "")
    }
  }
}
