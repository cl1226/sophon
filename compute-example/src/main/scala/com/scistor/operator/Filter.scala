package com.scistor.operator

import com.scistor.compute.interfacex.SparkProcessProxy
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class Filter extends SparkProcessProxy {
  override def transform(spark: SparkSession, table: Dataset[Row]): Dataset[Row] = {
    import spark.implicits._
    val df = table.toDF()
    val res = df.filter("name='tom'").filter($"address" === "上海")
    res
  }
}
