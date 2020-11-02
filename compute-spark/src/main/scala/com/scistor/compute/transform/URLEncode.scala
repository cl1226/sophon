package com.scistor.compute.transform

import java.net.URLEncoder

import com.scistor.compute.apis.BaseTransform
import com.scistor.compute.model.remote.TransStepDTO
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._

class URLEncode extends BaseTransform {

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
    (true, "")
  }

  /**
   * Allow to register user defined UDFs
   *
   * @return empty list if there is no UDFs to be registered
   * */
  override def getUdfList(): List[(String, UserDefinedFunction)] = {
    val func = udf((source: String) => URLEncoder.encode(source, "utf-8"))
    List(("urlencode", func))
  }

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {
    val decodeFunc = getUdfList().get(0)._2
    val sourceField = "source_field"
    val targetField = sourceField
    df.withColumn(targetField, decodeFunc(col(sourceField)))
  }
}
