package com.scistor.compute.transform

import com.scistor.compute.apis.BaseTransform
import com.scistor.compute.model.remote.TransStepDTO
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class Sample extends BaseTransform {

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
    val sampleDF = df.sample(true, attrs.get("fraction").asInstanceOf[Double])
    attrs.get("limit").asInstanceOf[Int] match {
      case -1 => sampleDF
      case limit: Int => sampleDF.limit(limit)
    }
  }

  /**
   * Return true and empty string if config is valid, return false and error message if config is invalid.
   */
  override def validate(): (Boolean, String) = {
    val attrs = config.getStepAttributes
    (attrs.containsKey("fraction")
      && attrs.containsKey("limit")
      && (attrs.get("fraction").asInstanceOf[Double] > 0.0))match {
      case true => (true, "")
      case false => (false, "please specify [fraction] as Double > 0 and [limit] as Integer")
    }
  }
}
