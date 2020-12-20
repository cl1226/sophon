package com.scistor.compute.transform

import com.scistor.compute.apis.BaseTransform
import com.scistor.compute.model.remote.TransStepDTO
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions.mapAsScalaMap

class Filter extends BaseTransform {

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
      println("\t" + value)
    })

    val condition = attrs.get("where").toString
    df.createOrReplaceTempView("scistor_tmp_filter")
    val res = spark.sql(s"select * from scistor_tmp_filter where ${condition}")
    res
  }

  /**
   * Return true and empty string if config is valid, return false and error message if config is invalid.
   */
  override def validate(): (Boolean, String) = {
    val attrs = config.getStepAttributes
    attrs.containsKey("where") match {
      case true => (true, "")
      case false => (false, s"please special filter conditions of ${config.getStepType} as string")
    }
  }
}
