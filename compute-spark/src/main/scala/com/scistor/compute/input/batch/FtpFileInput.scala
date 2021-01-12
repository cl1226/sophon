package com.scistor.compute.input.batch

import com.scistor.compute.apis.BaseStaticInput
import com.scistor.compute.model.remote.TransStepDTO
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._

class FtpFileInput extends BaseStaticInput {

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

  /**
   * Get DataFrame from this Static Input.
   * */
  override def getDataset(spark: SparkSession): Dataset[Row] = {
    import spark.implicits._

    val attrs = config.getStepAttributes

    println(s"[INFO] 输入数据源 [${config.getStepType}] properties: ")
    attrs.foreach(entry => {
      val (key, value) = entry
      println("\t" + key + " = " + value)
    })
    val username = attrs.getOrDefault("username", "").toString
    val password = attrs.getOrDefault("password", "").toString
    val url = attrs.getOrDefault("host", "").toString
    val port = attrs.getOrDefault("port", "21").toString
    val directory = attrs.getOrDefault("path", "").toString
    val datasource = s"ftp://${username}:${password}@${url}:${port}${directory}"

    val ftpInput: RDD[(String, PortableDataStream)] = spark.sparkContext.binaryFiles(datasource)

    ftpInput.map(x => (x._1, x._2.toArray())).toDF("ftpFileName", "ftpFileContent")
  }

  /**
   * Return true and empty string if config is valid, return false and error message if config is invalid.
   */
  override def validate(): (Boolean, String) = {
    val attrs = config.getStepAttributes
    if(!attrs.containsKey("host") || attrs.get("host").toString.equals("")) {
      (false, "please special [FtpFileInput] host as string")
    }
    if(!attrs.containsKey("path") || attrs.get("path").toString.equals("")) {
      (false, "please special [FtpFileInput] port as string")
    }
    (true, "")
  }

}
