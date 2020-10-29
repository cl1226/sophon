package com.scistor.compute.transform

import com.scistor.compute.apis.BaseTransform
import com.scistor.compute.model.spark.ComputeJob
import org.apache.spark.sql.functions.{col, crc32, md5, sha1}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class Checksum extends BaseTransform {

  var attribute: ComputeJob = _

  /**
   * Set Attribute.
   * */
  override def setAttribute(attr: ComputeJob): Unit = {
    this.attribute = attr
  }

  /**
   * get Attribute.
   * */
  override def getAttribute(): ComputeJob = attribute

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {
    val srcField = attribute.getAttrs.get("sourceField")
    val column = attribute.getAttrs.get("method").mkString.toUpperCase match {
      case "SHA1" => sha1(col(srcField))
      case "MD5" => md5(col(srcField))
      case "CRC32" => crc32(col(srcField))
    }
    df.withColumn(attribute.getAttrs.get("targetField").mkString, column)
  }

  /**
   * Return true and empty string if config is valid, return false and error message if config is invalid.
   */
  override def validate(): (Boolean, String) = {
    val allowedMethods = List("CRC32", "MD5", "SHA1")
    if (!attribute.getAttrs.containsKey("method")) {
      (false, "please specify [method] as a non-empty string")
    } else if(attribute.getAttrs.containsKey("method") && !allowedMethods.contains(attribute.getAttrs.get("method").mkString.trim.toUpperCase)) {
      (false, "method in [method] is not allowed, please specify one of " + allowedMethods.mkString(", "))
    } else if (!attribute.getAttrs.containsKey("sourceField")) {
      (false, "please specify [sourceField] as a non-empty string")
    } else if (!attribute.getAttrs.containsKey("targetField")) {
      (false, "please specify [targetField] as a non-empty string")
    } else {
      (true, "")
    }
  }
}
