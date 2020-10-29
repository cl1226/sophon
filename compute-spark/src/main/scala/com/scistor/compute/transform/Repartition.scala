package com.scistor.compute.transform

import com.scistor.compute.apis.BaseTransform
import com.scistor.compute.model.spark.ComputeJob
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class Repartition extends BaseTransform {

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

  /**
   * Return true and empty string if config is valid, return false and error message if config is invalid.
   */
  override def validate(): (Boolean, String) = {
    attribute.getAttrs.containsKey("numPartitions") && (attribute.getAttrs.get("numPartitions").toInt > 0) match {
      case true => (true, "")
      case false => (false, "please specify [numPartitions] as Integer > 0")
    }
  }

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {
    df.repartition(attribute.getAttrs.get("numPartitions").toInt)
  }

}
