package com.scistor.compute.transform

import com.alibaba.fastjson.JSON
import com.scistor.compute.apis.BaseTransform
import com.scistor.compute.model.remote.TransStepDTO
import com.scistor.compute.utils.HttpUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import java.util
import java.util.{ArrayList, HashMap, List, Map}
import scala.collection.JavaConversions._

class MLTextClass extends BaseTransform {

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

    val input: Array[String] = df.rdd.map(d => {
      val fileName: String = d.getAs[String]("ftpFileName")
      val fileContent: Array[Byte] = d.getAs[Array[Byte]]("ftpFileContent")
      val content = new String(fileContent, "utf-8")
      content
    }).collect

    val map: util.Map[String, Any] = new util.HashMap[String, Any]
    map.put("frame", attrs.getOrDefault("frame", "tensorflow2").toString)
    map.put("model", attrs.getOrDefault("model", "textcnn").toString)
    map.put("modelpath", attrs.getOrDefault("modelpath", "").toString)
    val childParam: util.Map[String, Any] = new util.HashMap[String, Any]
    childParam.put("class_num", attrs.getOrDefault("class_num", "0").toString.toInt)
    childParam.put("maxlen", attrs.getOrDefault("maxlen", "0").toString.toInt)
    childParam.put("embedding_dims", attrs.getOrDefault("embedding_dims", "0").toString.toInt)
    childParam.put("epochs", attrs.getOrDefault("epochs", "0").toString.toInt)
    childParam.put("batch_size", attrs.getOrDefault("batch_size", "0").toString.toInt)
    childParam.put("max_features", attrs.getOrDefault("max_features", "0").toString.toInt)
    map.put("modelparam", childParam)
    val contents: util.List[util.Map[String, Any]] = new util.ArrayList[util.Map[String, Any]]
    input.foreach(i => {
      val map: util.Map[String, Any] = new util.HashMap[String, Any]
      map.put("content", i)
      contents.add(map)
    })
    map.put("input", contents)

    val url: String = attrs.getOrDefault("url", "http://192.168.31.46:7777/api/text/classify/predict").toString
    val param: String = JSON.toJSON(map).toString
    val res: String = HttpUtils.doPost(url, param)
    val jsonObject = JSON.parseObject(res)
    val result = jsonObject.getString("result")
    val rdd = spark.sparkContext.parallelize(Array(result))
    val ds = spark.createDataset(rdd)
    val finalDF = spark.read.json(ds)

    finalDF.show()

    finalDF
  }

  /**
   * Return true and empty string if config is valid, return false and error message if config is invalid.
   */
  override def validate(): (Boolean, String) = {
    (true, "")
  }

}
