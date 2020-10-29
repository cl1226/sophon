package com.scistor.compute.transform

import com.scistor.compute.apis.BaseTransform
import com.scistor.compute.model.spark.ComputeJob
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class Join extends BaseTransform {

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
    val sourceTableName = attribute.getAttrs.get("sourceTableName").mkString
    val targetTableName = attribute.getAttrs.get("targetTableName").mkString
    val joinField: Seq[String] = attribute.getAttrs.get("joinField").mkString.split(",").toSeq
    val joinType = attribute.getAttrs.getOrDefault("joinType", "inner").mkString
    val sourceDF = spark.read.table(sourceTableName)
    val targetDF = spark.read.table(targetTableName)
    val df = sourceDF.join(targetDF, joinField, joinType)
    println("[INFO] join result: ")
    df.show()
    df
  }

  /**
   * Return true and empty string if config is valid, return false and error message if config is invalid.
   */
  override def validate(): (Boolean, String) = {
    if (!attribute.getAttrs.containsKey("sourceTableName")) {
      (false, "please specify [sourceTableName] as a non-empty string")
    } else if (!attribute.getAttrs.containsKey("targetTableName")) {
      (false, "please specify [targetTableName] as a non-empty string")
    } else if(!attribute.getAttrs.containsKey("joinField")) {
      (false, "please specify [joinField] as a non-empty string")
    } else {
      (true, "")
    }
  }
}
