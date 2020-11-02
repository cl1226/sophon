package com.scistor.compute.transform

import com.scistor.compute.apis.BaseTransform
import com.scistor.compute.model.remote.TransStepDTO
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class Repartition extends BaseTransform {

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
    attrs.containsKey("numPartitions") && (attrs.get("numPartitions").asInstanceOf[Int] > 0) match {
      case true => (true, "")
      case false => (false, "please specify [numPartitions] as Integer > 0")
    }
  }

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {
    val attrs = config.getStepAttributes
    df.repartition(attrs.get("numPartitions").asInstanceOf[Int])
  }

}