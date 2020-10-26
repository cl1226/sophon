package com.scistor.compute.transform

import com.scistor.compute.apis.Plugin
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.expressions.{UserDefinedAggregateFunction, UserDefinedFunction}

abstract class BaseTransform extends Plugin {

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
