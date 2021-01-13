package com.scistor.compute.transform

import com.alibaba.fastjson.JSON
import com.scistor.compute.apis.BaseTransform
import com.scistor.compute.model.remote.TransStepDTO
import com.scistor.compute.utils.HttpUtils
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import sun.misc.BASE64Encoder

import java.util
import java.util.Base64
import scala.collection.JavaConversions._

class MLImgClass extends BaseTransform {

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

    val input: Array[(String, String)] = df.rdd.map(d => {
      val fileName: String = d.getAs[String]("ftpFileName")
      val fileContent: Array[Byte] = d.getAs[Array[Byte]]("ftpFileContent")
      val content = new BASE64Encoder().encode(fileContent)
      (fileName, content)
    }).collect

    val map: util.Map[String, Any] = new util.HashMap[String, Any]
    map.put("frame", attrs.getOrDefault("frame", "pytorch").toString)
    map.put("model", attrs.getOrDefault("model", "yolov5s.pt").toString)
    map.put("image_save", attrs.getOrDefault("image_save", "false"))

    val arr = new util.ArrayList[util.Map[String, String]]()
    input.foreach(i => {
      val m = new util.HashMap[String, String]()
      m.put("url", i._1)
      m.put("data", i._2)
      arr.add(m)
    })
    map.put("image_data", arr)

    val url: String = attrs.getOrDefault("url", "http://192.168.31.46:7777/api/picture/classify").toString
    val param: String = JSON.toJSON(map).toString

    val res: String = HttpUtils.doPost(url, param)
    val jsonObject = JSON.parseObject(res)
    val result = jsonObject.getString("result")
    val rdd = spark.sparkContext.parallelize(Array(result))
    val ds = spark.createDataset(rdd)
    val finalDF = spark.read.json(ds)

    finalDF
  }

  /**
   * Return true and empty string if config is valid, return false and error message if config is invalid.
   */
  override def validate(): (Boolean, String) = {
    (true, "")
  }

}
