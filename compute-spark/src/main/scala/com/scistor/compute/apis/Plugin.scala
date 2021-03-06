package com.scistor.compute.apis

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
 * checkConfig --> prepare
 */
trait Plugin extends Serializable with Logging {

  /**
   * Get Plugin Name.
   */
  def name: String = this.getClass.getName

  /**
   * Prepare before running, do things like set config default value, add broadcast variable, accumulator.
   */
  def prepare(spark: SparkSession): Unit = {}

  /**
   *  Return true and empty string if config is valid, return false and error message if config is invalid.
   */
  def validate(): (Boolean, String)

}
