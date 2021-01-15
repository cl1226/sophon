package com.scistor.compute.iprange

import com.scistor.compute.interfacex.SparkProcessProxy
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
 * 原表[1.0.0.0, a, b, c]
 * ip表:
 * [1.0.0.0, 1.0.0.1, a, a]
 * [1.0.0.2, 1.0.0.3, b, b]
 * [1.0.0.4, 1.0.0.5, c, c]
 * 将原表中的ip列，在ip表中找出所在行范围
 */
class FlatMapIP extends SparkProcessProxy {

  def binarySearch(arr: Array[(String, String, String, String)], ip: Long): Int = {
    var l = 0
    var h = arr.length - 1
    while (l <= h) {
      var m = (l + h) / 2
      if ((ip >= arr(m)._1.toLong) && (ip <= arr(m)._2.toLong)) {
        return m
      } else if (ip < arr(m)._1.toLong) {
        h = m - 1
      } else {
        l = m + 1
      }
    }
    -1
  }

  override def transform(spark: SparkSession, table: Dataset[Row]): Dataset[Row] = {
    val ipframe = spark.sql("select * from iprange")
    val iprange = ipframe.rdd.map(x => {
      (x.getAs[String]("startip_long"),
        x.getAs[String]("endip_long"),
        x.getAs[String]("country"),
        x.getAs[String]("local")
      )
    })

    val sc = spark.sparkContext
    val bd = sc.broadcast(iprange.collect)
    val source = spark.sql("select * from source")
    val schema = source.schema
      .add(StructField("aaa", StringType, true))
      .add(StructField("bbb", StringType, true))
      .add(StructField("ccc", StringType, true))
      .add(StructField("ddd", StringType, true))
    val value: RDD[Row] = source.rdd.map(x => {
      val mscid_long = x.getAs[String]("mscid_long")
      val index = binarySearch(bd.value, mscid_long.toLong)
      val originalColumns = x.toSeq.toList
      if (index == -1) {
        Row.fromSeq(originalColumns :+ "" :+ "" :+ "" :+ "")
      } else {
        Row.fromSeq(originalColumns
          :+ bd.value(index)._3
          :+ bd.value(index)._4
          :+ bd.value(index)._1
          :+ bd.value(index)._2
        )
      }
    })
    val df = spark.createDataFrame(value, schema)
    df

  }
}
