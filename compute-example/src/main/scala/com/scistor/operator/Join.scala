package com.scistor.operator

import com.scistor.compute.interfacex.SparkProcessProxy2
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class Join extends SparkProcessProxy2 {
  override def transform(spark: SparkSession, table1: Dataset[Row], table2: Dataset[Row]): Dataset[Row] = {

    table1.join(table2, table1("name") === table2("name"))

  }
}
