package com.scistor.operator

import com.scistor.compute.interfacex.SparkProcessProxy
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class ParamCombine1 extends SparkProcessProxy {
  override def transform(spark: SparkSession, table: Dataset[Row]): Dataset[Row] = {
    import spark.implicits._

    val schema = StructType(
      Seq(StructField("param1", StringType)
      )
    )

    val df = spark.createDataFrame(table.rdd, schema)

    val ds = df.map(row => {
      val value1 = row.getAs[String]("param1")
      value1
    })

    val tuple = ds.rdd.reduce((x1, x2) => {
      x1 + "," + x2
    })

    val value = spark.sparkContext.parallelize(Seq(tuple))
    val value1 = spark.createDataset(value)
    val res = value1.toDF("param1")
    res
  }
}
