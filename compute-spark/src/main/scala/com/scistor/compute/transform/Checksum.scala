package com.scistor.compute.transform

import com.scistor.compute.apis.BaseTransform
import com.scistor.compute.model.remote.TransStepDTO
import org.apache.spark.sql.functions.{col, crc32, md5, sha1}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class Checksum extends BaseTransform {

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
    val attrs = config.getStepAttributes
    val srcField = attrs.get("sourceField").toString
    val column = attrs.get("method").toString.toUpperCase match {
      case "SHA1" => sha1(col(srcField))
      case "MD5" => md5(col(srcField))
      case "CRC32" => crc32(col(srcField))
    }
    df.withColumn(attrs.get("targetField").toString, column)
  }

  /**
   * Return true and empty string if config is valid, return false and error message if config is invalid.
   */
  override def validate(): (Boolean, String) = {
    val attrs = config.getStepAttributes
    val allowedMethods = List("CRC32", "MD5", "SHA1")
    if (!attrs.containsKey("method")) {
      (false, "please specify [method] as a non-empty string")
    } else if(attrs.containsKey("method") && !allowedMethods.contains(attrs.get("method").toString.trim.toUpperCase)) {
      (false, "method in [method] is not allowed, please specify one of " + allowedMethods.mkString(", "))
    } else if (!attrs.containsKey("sourceField")) {
      (false, "please specify [sourceField] as a non-empty string")
    } else if (!attrs.containsKey("targetField")) {
      (false, "please specify [targetField] as a non-empty string")
    } else {
      (true, "")
    }
  }
}
