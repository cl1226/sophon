package com.scistor.compute.input.batch

import com.scistor.compute.apis.BaseStaticInput
import com.scistor.compute.model.spark.SourceAttribute
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

class Hbase extends BaseStaticInput {

  var source: SourceAttribute = _

  /**
   * Set SourceAttribute.
   **/
  override def setSource(source: SourceAttribute): Unit = {
    this.source = source
  }

  /**
   * get SourceAttribute.
   **/
  override def getSource(): SourceAttribute = {
    this.source
  }

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

    var df: Dataset[Row] = null
    df
  }
}
