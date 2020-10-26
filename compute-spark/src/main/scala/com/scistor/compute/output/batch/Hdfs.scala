package com.scistor.compute.output.batch
import org.apache.spark.sql.{Dataset, Row}

class Hdfs extends FileOutputBase {

  override def process(df: Dataset[Row]): Unit = {
    super.processImpl(df, "hdfs://")
  }

}
