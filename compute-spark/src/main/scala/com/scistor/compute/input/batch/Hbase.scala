package com.scistor.compute.input.batch

import com.scistor.compute.apis.BaseStaticInput
import com.scistor.compute.model.remote.TransStepDTO
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class Hbase extends BaseStaticInput {

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
   **/
  override def getDataset(spark: SparkSession): Dataset[Row] = {

//    val config = HBaseConfiguration.create()
//    config.set(HConstants.ZOOKEEPER_QUORUM, source.connection_url.split(":")(0))
//    config.set(HConstants.ZOOKEEPER_CLIENT_PORT, source.connection_url.split(":")(1))
//    config.set(org.apache.hadoop.hbase.mapreduce.TableInputFormat.INPUT_TABLE, source.table_name)
    spark.read.format("jdbc")
      .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver")
    var df: Dataset[Row] = null
    df
  }
}
