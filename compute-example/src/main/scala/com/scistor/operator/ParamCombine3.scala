package com.scistor.operator

import com.scistor.compute.interfacex.SparkProcessProxy
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class ParamCombine3 extends SparkProcessProxy {
  override def transform(spark: SparkSession, table: Dataset[Row]): Dataset[Row] = {
    import spark.implicits._

    val schema = StructType(
      Seq(StructField("param1", StringType),
        StructField("param2", StringType),
        StructField("param3", StringType)
      )
    )

    val df = spark.createDataFrame(table.rdd, schema)

    val ds: Dataset[(String, String, String)] = df.map(row => {
      val value1 = row.getAs[String]("param1")
      val value2 = row.getAs[String]("param2")
      val value3 = row.getAs[String]("param3")
      (value1, value2, value3)
    })

    val tuple3 = ds.rdd.reduce((x1, x2) => {
      (x1._1 + "," + x2._1, x1._2 + "," + x2._2, x1._3 + "," + x2._3)
    })

    val value = spark.sparkContext.parallelize(Seq((tuple3._1, tuple3._2, tuple3._3)))
    val value1 = spark.createDataset(value)
    val res = value1.toDF("param1", "param2", "param3")
    res
  }
}
