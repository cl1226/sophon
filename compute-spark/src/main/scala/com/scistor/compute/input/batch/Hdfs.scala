package com.scistor.compute.input.batch

import org.apache.spark.sql.{Dataset, Row, SparkSession}

class Hdfs extends File {
  /**
   * Get DataFrame from this Static Input.
   **/
  override def getDataset(spark: SparkSession): Dataset[Row] = {
    val attrs = config.getStepAttributes
    val path = buildPathWithDefaultSchema(attrs.get("connectAddress").toString, "hdfs://")
    fileReader(spark, path)
  }
}
