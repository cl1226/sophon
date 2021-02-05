package com.scistor.compute.input.batch

import com.redislabs.provider.redis.{RedisConfig, RedisEndpoint, toRedisContext}
import com.scistor.compute.apis.BaseStaticInput
import com.scistor.compute.model.remote.TransStepDTO
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._

class Redis extends BaseStaticInput {

  val defaultPort: Int = 6379
  val defaultDb: Int = 0
  val defaultPartition: Int = 3

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
   * Get DataFrame from this Static Input.
   * */
  override def getDataset(spark: SparkSession): Dataset[Row] = {
    val attrs = config.getStepAttributes

    println(s"[INFO] 输入数据源 [${config.getStepType}] properties: ")
    attrs.foreach(entry => {
      val (key, value) = entry
      println("\t" + key + " = " + value)
    })

    val host = attrs.get("host").toString
    val port = attrs.getOrDefault("port", defaultPort).toString.toInt
    val keyPattern = attrs.get("key_pattern").toString
    val partition = attrs.getOrDefault("partition", defaultPartition).toString.toInt
    val dbNum = attrs.getOrDefault("db_num", defaultDb).toString.toInt
    val redisConfig = new RedisConfig(new RedisEndpoint(host = host, port = port, dbNum = dbNum))
    val redisRDD = spark.sparkContext.fromRedisKV(keyPattern, partition)(redisConfig = redisConfig)

    import spark.implicits._
    val df = redisRDD.toDF("raw_key", "raw_message")
    df
  }

}
