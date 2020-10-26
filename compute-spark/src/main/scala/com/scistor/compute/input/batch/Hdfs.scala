package com.scistor.compute.input.batch

import org.apache.spark.sql.{Dataset, Row, SparkSession}

class Hdfs extends File {
  /**
   * Get DataFrame from this Static Input.
   **/
  override def getDataset(spark: SparkSession): Dataset[Row] = {
    val path = buildPathWithDefaultSchema(sourceAttribute.connection_url, "hdfs://")
    fileReader(spark, path)
  }
}
