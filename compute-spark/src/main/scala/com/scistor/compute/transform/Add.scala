package com.scistor.compute.transform

import com.scistor.compute.apis.BaseTransform
import com.scistor.compute.model.remote.TransStepDTO
import com.scistor.compute.model.spark.ComputeJob
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import scalaj.http
import scalaj.http.Http

import scala.collection.JavaConverters._
import scala.collection.mutable

class Add extends BaseTransform {

  var config: TransStepDTO = _

  /**
   * Set Config.
   * */
  override def setConfig(config: TransStepDTO): Unit = {
    this.config = config
  }

  /**
   * Get Config.
   * */
  override def getConfig(): TransStepDTO = config

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {
    import spark.implicits._

//    config.getStepAttributes
//    attribute.getAttrs.asScala.foreach(attr => {
//      attr._2.startsWith("http://") match {
//        case true => {
//          val response: http.HttpResponse[String] = Http(attr._2).header("Accept", "application/json").timeout(10000, 1000).asString
//          val rdd = spark.sparkContext.parallelize(Seq(response.body))
//          val strs = attr._2.split("\\.")
//          val format = strs(strs.length - 1)
//          val reader = spark.read.format(format)
//          format match {
//            case "json" => {
//              val x = rdd.flatMap(_.split("\n"))
//              reader.option("mode", "PERMISSIVE").json(x)
//            }
//            case "csv" => {
//              val x = rdd.flatMap(_.split("\r\n"))
//              reader.option("header", "true").csv(x.toDS())
//            }
//            case "txt" => {
//              val x = rdd.flatMap(_.split("\r\n"))
//              spark.createDataset(x)
//            }
//          }
//        }
//        case _ => df.withColumn(attr._1, lit(attr._2))
//      }
//    })
    df
  }

  /**
   * Return true and empty string if config is valid, return false and error message if config is invalid.
   */
  override def validate(): (Boolean, String) = {
    (true, "")
  }
}
