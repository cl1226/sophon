package com.scistor.compute.transform

import com.scistor.compute.apis.BaseTransform
import com.scistor.compute.model.remote.TransStepDTO
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class Join extends BaseTransform {

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
    val sourceTableName = attrs.get("sourceTableName").toString
    val targetTableName = attrs.get("targetTableName").toString
    val joinField: Seq[String] = attrs.get("joinField").toString.split(",").toSeq
    val joinType = attrs.getOrDefault("joinType", "inner").toString
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
    val attrs = config.getStepAttributes
    if (!attrs.containsKey("sourceTableName")) {
      (false, "please specify [sourceTableName] as a non-empty string")
    } else if (!attrs.containsKey("targetTableName")) {
      (false, "please specify [targetTableName] as a non-empty string")
    } else if(!attrs.containsKey("joinField")) {
      (false, "please specify [joinField] as a non-empty string")
    } else {
      (true, "")
    }
  }
}
