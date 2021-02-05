package com.scistor.compute.input.sparkstreaming

import com.scistor.compute.apis.BaseStreamingInput
import com.scistor.compute.input.sparkstreaming.receiver.redis.{RedisInfo, RedisReceiver}
import com.scistor.compute.model.remote.TransStepDTO
import org.apache.calcite.avatica.ColumnMetaData.StructType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, RowFactory, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import scala.collection.JavaConversions._

class RedisStream extends BaseStreamingInput[String] {

  var config: TransStepDTO = _

  /**
   * Set Config.
   **/
  override def setConfig(config: TransStepDTO): Unit = {
    this.config = config
  }

  /**
   * Get Config.
   **/
  override def getConfig(): TransStepDTO = config

  /**
   * Return true and empty string if config is valid, return false and error message if config is invalid.
   */
  override def validate(): (Boolean, String) = {
    (true, "")
  }

  /**
   * This must be implemented to convert RDD[T] to Dataset[Row] for later processing
   * */
  override def rdd2dataset(spark: SparkSession, rdd: RDD[String]): Dataset[Row] = {
    val rowsRDD = rdd.map(element => {
      RowFactory.create(element)
    })
    val schema = StructType(Array(StructField("raw_message", DataTypes.StringType)))
    spark.createDataFrame(rowsRDD, schema)
  }

  /**
   * Create spark dstream from data source, you can specify type parameter.
   * */
  override def getDStream(ssc: StreamingContext): DStream[String] = {
    val attrs = config.getStepAttributes

    println(s"[INFO] 输入数据源 [${config.getStepType}] properties: ")
    attrs.foreach(entry => {
      val (key, value) = entry
      println("\t" + key + " = " + value)
    })
    val redisInfo = new RedisInfo(attrs)
    ssc.receiverStream(new RedisReceiver(redisInfo))
  }
}
