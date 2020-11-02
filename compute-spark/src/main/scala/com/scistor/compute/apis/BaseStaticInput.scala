package com.scistor.compute.apis

import com.scistor.compute.model.remote.TransStepDTO
import org.apache.spark.sql.{Dataset, Row, SparkSession}

abstract class BaseStaticInput extends Plugin {

  /**
   * Set Config.
   * */
  def setConfig(config: TransStepDTO)

  /**
   * Get Config.
   * */
  def getConfig(): TransStepDTO


  /**
   * Get DataFrame from this Static Input.
   * */
  def getDataset(spark: SparkSession): Dataset[Row]

}
