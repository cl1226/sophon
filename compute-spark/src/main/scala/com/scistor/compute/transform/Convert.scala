package com.scistor.compute.transform

import com.scistor.compute.apis.BaseTransform
import com.scistor.compute.model.spark.ComputeJob
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{BooleanType, DoubleType, FloatType, IntegerType, LongType, StringType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class Convert extends BaseTransform {

  var attribute: ComputeJob = _

  /**
   * Set Attribute.
   * */
  override def setAttribute(attr: ComputeJob): Unit = {
    this.attribute = attr
  }

  /**
   * get Attribute.
   * */
  override def getAttribute(): ComputeJob = attribute

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {
    val srcField = attribute.getAttrs.get("sourceField").mkString
    val newType = attribute.getAttrs.get("newType").mkString

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
    if (!attribute.getAttrs.containsKey("sourceField")) {
      (false, "please specify [sourceType] as a non-empty string")
    } else if (!attribute.getAttrs.containsKey("newType")) {
      (false, "please specify [newType] as a non-empty string")
    } else {
      (true, "")
    }
  }
}
