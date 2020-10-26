package com.scistor.compute.apis

import com.scistor.compute.model.spark.SinkAttribute
import org.apache.spark.sql.{Dataset, Row}

abstract class BaseOutput extends Plugin {

  /**
   * Set SinkAttribute.
   * */
  def setSink(sink: SinkAttribute)

  /**
   * get SinkAttribute.
   * */
  def getSink(): SinkAttribute

  def process(df: Dataset[Row])

}
