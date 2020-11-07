package com.scistor.compute.input.batch

import org.apache.spark.sql.{Dataset, Row, SparkSession}

class Hdfs extends File {

  /**
   * Return true and empty string if config is valid, return false and error message if config is invalid.
   */
  override def validate(): (Boolean, String) = {
    val attrs = config.getStepAttributes
    attrs.containsKey("connectAddress") match {
      case true => {
        // TODO CHECK hosts
        (true, "")
      }
      case false => (false, "please specify [catalog] as a non-empty string")
    }
    attrs.containsKey("dataFormatType") match {
      case true => {
        (true, "")
      }
      case false => (false, "please specify [format] as a of [text, json, csv, parquet, orc]")
    }
  }

  /**
   * Get DataFrame from this Static Input.
   **/
  override def getDataset(spark: SparkSession): Dataset[Row] = {
    val attrs = config.getStepAttributes
    val path = buildPathWithDefaultSchema(attrs.get("connectAddress").toString, "hdfs://")
    fileReader(spark, path)
  }
}
