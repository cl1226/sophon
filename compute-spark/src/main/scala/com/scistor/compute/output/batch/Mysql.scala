package com.scistor.compute.output.batch

import com.scistor.compute.apis.BaseOutput
import com.scistor.compute.model.spark.{SinkAttribute, SourceAttribute}
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

class Mysql extends BaseOutput {

  var firstProcess = true

  var sink: SinkAttribute = _

  /**
   * Set SinkAttribute.
   **/
  override def setSink(sink: SinkAttribute): Unit = {
    this.sink = sink
  }

  /**
   * get SinkAttribute.
   **/
  override def getSink(): SinkAttribute = {
    this.sink
  }

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)
  }

  override def process(df: Dataset[Row]): Unit = {
    val parameters = sink.parameters
    val saveMode = parameters.getOrDefault("saveMode", "append")

    val prop = new java.util.Properties
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    prop.setProperty("user", sink.sink_connection_username)
    prop.setProperty("password", sink.sink_connection_password)

    df.write.mode(saveMode).jdbc(sink.sink_connection_url, sink.tableName, prop)

  }

}
