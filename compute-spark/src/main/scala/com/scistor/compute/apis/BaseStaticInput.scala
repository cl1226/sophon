package com.scistor.compute.apis

import com.scistor.compute.model.spark.SourceAttribute
import org.apache.spark.sql.{Dataset, Row, SparkSession}

abstract class BaseStaticInput extends Plugin {

  /**
   * Set SourceAttribute.
   * */
  def setSource(source: SourceAttribute)

  /**
   * get SourceAttribute.
   * */
  def getSource(): SourceAttribute


  /**
   * Get DataFrame from this Static Input.
   * */
  def getDataset(spark: SparkSession): Dataset[Row]

}
