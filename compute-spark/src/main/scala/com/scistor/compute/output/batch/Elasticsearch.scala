package com.scistor.compute.output.batch

import com.scistor.compute.apis.BaseOutput
import com.scistor.compute.model.spark.SinkAttribute
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.elasticsearch.spark.sql._

class Elasticsearch extends BaseOutput {

  var esCfg: Map[String, String] = Map()
  val esPrefix = "es."

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


  /**
   * Prepare before running, do things like set config default value, add broadcast variable, accumulator.
   */
  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    esCfg += ("index" -> sink.getDatabaseName)
    esCfg += ("index_type" -> sink.getTableName)
    esCfg += ("index_time_format" -> "yyyy.MM.dd")
    esCfg += ("es.nodes" -> sink.sink_connection_url.split(":")(0))

    if(StringUtils.isNoneBlank(sink.sink_connection_username)) {
      esCfg += ("es.net.http.auth.user" -> sink.sink_connection_username)
    }
    if(StringUtils.isNoneBlank(sink.sink_connection_password)) {
      esCfg += ("es.net.http.auth.pass" -> sink.sink_connection_password)
    }

    println("[INFO] Output ElasticSearch Params:")
    for (entry <- esCfg) {
      val (key, value) = entry
      println("\t" + key + " = " + value)
    }
  }

  override def process(df: Dataset[Row]): Unit = {
    df.saveToEs(esCfg.get("index").get + "/" + esCfg.get("index_type").get, this.esCfg)
  }
}
