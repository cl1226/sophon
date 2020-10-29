package com.scistor.compute.transform

import com.scistor.compute.apis.BaseTransform
import com.scistor.compute.model.spark.ComputeJob
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class Sample extends BaseTransform {

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
    val sampleDF = df.sample(true, attribute.getAttrs.get("fraction").toDouble)
    attribute.getAttrs.get("limit").toInt match {
      case -1 => sampleDF
      case limit: Int => sampleDF.limit(limit)
    }
  }

  /**
   * Return true and empty string if config is valid, return false and error message if config is invalid.
   */
  override def validate(): (Boolean, String) = {
    (attribute.getAttrs.containsKey("fraction")
      && attribute.getAttrs.containsKey("limit")
      && (attribute.getAttrs.get("fraction").toDouble > 0.0))match {
      case true => (true, "")
      case false => (false, "please specify [fraction] as Double > 0 and [limit] as Integer")
    }
  }
}
