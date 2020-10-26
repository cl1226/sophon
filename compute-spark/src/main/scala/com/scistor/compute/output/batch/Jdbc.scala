package com.scistor.compute.output.batch

import com.scistor.compute.apis.BaseOutput
import com.scistor.compute.model.spark.SinkAttribute
import org.apache.spark.sql.{Dataset, Row, SaveMode}

class Jdbc extends BaseOutput {

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

  override def process(df: Dataset[Row]): Unit = {
    val prop = new java.util.Properties
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    prop.setProperty("user", sink.sink_connection_username)
    prop.setProperty("password", sink.sink_connection_password)

    val saveMode = "append"

    if (firstProcess) {
      df.write.mode(saveMode).jdbc(sink.sink_connection_url, sink.tableName, prop)
      firstProcess = false
    } else if (saveMode == "overwrite") {
      // actually user only want the first time overwrite in streaming(generating multiple dataframe)
      df.write.mode(SaveMode.Append).jdbc(sink.sink_connection_url, sink.tableName, prop)
    } else {
      df.write.mode(saveMode).jdbc(sink.sink_connection_url, sink.tableName, prop)
    }
  }
}
