package com.scistor.operator

import com.scistor.compute.interfacex.SparkProcessProxy
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class ParamCombine2 extends SparkProcessProxy {
  override def transform(spark: SparkSession, table: Dataset[Row]): Dataset[Row] = {
    import spark.implicits._

    val schema = StructType(
      Seq(StructField("param1", StringType),
        StructField("param2", StringType)
      )
    )

    val df = spark.createDataFrame(table.rdd, schema)

    val ds = df.map(row => {
      val value1 = row.getAs[String]("param1")
      val value2 = row.getAs[String]("param2")
      (value1, value2)
    })

    val tuple2 = ds.rdd.reduce((x1, x2) => {
      (x1._1 + "," + x2._1, x1._2 + "," + x2._2)
    })

    val value = spark.sparkContext.parallelize(Seq((tuple2._1, tuple2._2)))
    val value1 = spark.createDataset(value)
    val res = value1.toDF("param1", "param2")
    res
  }
}
