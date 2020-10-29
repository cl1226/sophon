package com.scistor.compute.input.batch

import com.scistor.compute.apis.BaseStaticInput
import com.scistor.compute.model.spark.SourceAttribute
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class Hive extends BaseStaticInput {

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
    val table = source.table_name
    spark.sql(s"select * from ${table}")
  }
}
