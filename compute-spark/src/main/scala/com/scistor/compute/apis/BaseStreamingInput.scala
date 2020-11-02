package com.scistor.compute.apis

import com.scistor.compute.model.remote.TransStepDTO
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

abstract class BaseStreamingInput[T] extends Plugin {

  /**
   * Set Config.
   * */
  def setConfig(config: TransStepDTO)

  /**
   * get Config.
   * */
  def getConfig(): TransStepDTO

  /**
   * This must be implemented to convert RDD[T] to Dataset[Row] for later processing
   * */
  def rdd2dataset(spark: SparkSession, rdd: RDD[T]): Dataset[Row]

  /**
   * start should be invoked in when data is ready.
   * */
  def start(spark: SparkSession, ssc: StreamingContext, handler: Dataset[Row] => Unit): Unit = {

    getDStream(ssc).foreachRDD(rdd => {
      val dataset = rdd2dataset(spark, rdd)
      handler(dataset)
    })
  }

  /**
   * Create spark dstream from data source, you can specify type parameter.
   * */
  def getDStream(ssc: StreamingContext): DStream[T]

}
