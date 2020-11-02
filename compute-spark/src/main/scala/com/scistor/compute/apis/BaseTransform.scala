package com.scistor.compute.apis

import com.scistor.compute.model.remote.TransStepDTO
import org.apache.spark.sql.expressions.{UserDefinedAggregateFunction, UserDefinedFunction}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

abstract class BaseTransform extends Plugin {

  /**
   * Set Config.
   * */
  def setConfig(config: TransStepDTO)

  /**
   * Get Config.
   * */
  def getConfig(): TransStepDTO

  def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row]

  /**
   * Allow to register user defined UDFs
   * @return empty list if there is no UDFs to be registered
   * */
  def getUdfList(): List[(String, UserDefinedFunction)] = List.empty

  /**
   * Allow to register user defined UDAFs
   * @return empty list if there is no UDAFs to be registered
   * */
  def getUdafList(): List[(String, UserDefinedAggregateFunction)] = List.empty

}
