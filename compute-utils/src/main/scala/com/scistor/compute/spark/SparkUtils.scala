package com.scistor.compute.spark

import java.util.Properties

object SparkUtils {

  private val sparkCommand = new SparkCommand;

  def init(properties: Properties) = {
    sparkCommand.init(properties)
    this
  }

  def build(jobName: String): Unit = {

  }

  def submit(): Unit = {

  }

}
