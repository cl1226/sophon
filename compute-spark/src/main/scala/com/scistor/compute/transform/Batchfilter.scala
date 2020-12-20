package com.scistor.compute.transform

import com.scistor.compute.apis.BaseTransform
import com.scistor.compute.model.remote.TransStepDTO
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import scalaj.http
import scalaj.http.Http

import scala.collection.JavaConversions._

class Batchfilter extends BaseTransform {

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

    val attrs = config.getStepAttributes

    println(s"[INFO] 转换算子 [${config.getStepType}] properties: ")
    attrs.foreach(entry => {
      val (key, value) = entry
      println("\t" + key + " = " + value)
    })

    val response: http.HttpResponse[String] = Http(attrs.get("fileUrl").toString).header("Accept", "application/json").timeout(10000, 1000).asString
    val result = response.body

    val rdd = spark.sparkContext.parallelize(result.split("\r\n"))

    val ds = spark.createDataset(rdd)
    val batchDF = spark.read.option("header", "true").csv(ds)

    var seq = Seq[String]()
    attrs.get("filter").asInstanceOf[java.util.List[java.util.Map[String, String]]].foreach(f => {
      seq = seq :+ f.get("value")
    })
    val res = df.join(batchDF, seq)
    batchDF.columns.foreach(col => res.drop(col))
    res
  }

  /**
   * Return true and empty string if config is valid, return false and error message if config is invalid.
   */
  override def validate(): (Boolean, String) = {
    val attrs = config.getStepAttributes
    if (!attrs.containsKey("fileUrl")) {
      (false, "please upload batch filter file as a non-empty csv")
    } else {
      (true, "")
    }
  }
}
