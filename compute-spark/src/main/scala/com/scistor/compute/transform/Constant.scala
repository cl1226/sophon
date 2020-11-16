package com.scistor.compute.transform

import java.util
import com.scistor.compute.apis.BaseTransform
import com.scistor.compute.model.remote.TransStepDTO
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConverters._

/**
 * add constant value to dataset
 */
class Constant extends BaseTransform {

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
    var finalDF = df
    val attrs = config.getStepAttributes
    val constantMap = attrs.get("constantMap").asInstanceOf[util.Map[String, AnyRef]]
    constantMap.asScala.foreach(attr => {
      finalDF = finalDF.withColumn(attr._1, lit(attr._2))
    })
    finalDF
  }

  /**
   * Return true and empty string if config is valid, return false and error message if config is invalid.
   */
  override def validate(): (Boolean, String) = {
    val attrs = config.getStepAttributes
    attrs.containsKey("constantMap") match {
      case true => (true, "")
      case false => (false, s"please specify [constant] in ${attrs.get("dataSourceType")} as a non-empty string")
    }
  }
}
