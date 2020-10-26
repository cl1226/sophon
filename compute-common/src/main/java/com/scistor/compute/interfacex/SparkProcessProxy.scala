package com.scistor.compute.interfacex

import org.apache.spark.sql.{Dataset, Row, SparkSession}

trait SparkProcessProxy extends Serializable {
  def transform(spark: SparkSession, table: Dataset[Row]): Dataset[Row]
}
