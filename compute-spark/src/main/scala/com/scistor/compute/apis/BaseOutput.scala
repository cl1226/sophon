package com.scistor.compute.apis

import com.scistor.compute.model.remote.TransStepDTO
import org.apache.spark.sql.{Dataset, Row}

abstract class BaseOutput extends Plugin {

  /**
   * Set Config.
   * */
  def setConfig(config: TransStepDTO)

  /**
   * Get Config.
   * */
  def getConfig(): TransStepDTO

  def process(df: Dataset[Row])

}
