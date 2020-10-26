package com.scistor.compute.interfacex

import org.apache.spark.sql.{Dataset, Row, SparkSession}

trait SparkProcessProxy2 extends Serializable {

  def transform(spark: SparkSession, table1: Dataset[Row], table2: Dataset[Row]): Dataset[Row]

}
