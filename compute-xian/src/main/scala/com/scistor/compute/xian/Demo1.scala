package com.scistor.compute.xian

import com.scistor.compute.interfacex.SparkProcessProxy
import com.wuxi.scistor.analysis.Analysis
import org.apache.spark.sql.types.{ArrayType, ByteType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
 * 调用西安的数据处理程序，将数据转换成spark的dataframe
 */
class Demo1 extends SparkProcessProxy {
  override def transform(spark: SparkSession, table: Dataset[Row]): Dataset[Row] = {

    val analysis = new Analysis

    val res = table.rdd.map(row => {
      val fileName = row.getAs[String]("ftpFileName")
      val fileContent = row.getAs[Array[Byte]]("ftpFileContent")
      val maps = analysis.analysis(fileContent).asScala
      val rows = maps.map(m => Row(m.values.toSeq: _*))
      val header = maps.head.keys.toList
      val schema = StructType(header.map(fieldName => StructField(fieldName, StringType, true)))
      (fileName, (schema, rows))
    })

    val temp = res.collect.map(r => {
      val rdd = spark.sparkContext.parallelize(r._2._2)
      val frame = spark.createDataFrame(rdd, r._2._1)
      (r._1, (r._2._1, frame))
    })

    var result: DataFrame = null

    temp.foreach(x => {
      var finalDF: DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], x._2._1)
      finalDF = finalDF.union(x._2._2)
      result = finalDF
    })

    result

  }
}
