package com.scistor.compute.output.batch

import com.scistor.compute.apis.BaseOutput
import com.scistor.compute.model.spark.SinkAttribute
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

    esCfg += ("index" -> "scistor")
    esCfg += ("index_type" -> "log")
    esCfg += ("index_time_format" -> "yyyy.MM.dd")
    esCfg += ("es.nodes" -> sink.sink_connection_url.mkString(","))

    println("[INFO] Output ElasticSearch Params:")
    for (entry <- esCfg) {
      val (key, value) = entry
      println("[INFO] \t" + key + " = " + value)
    }
  }

  override def process(df: Dataset[Row]): Unit = {
    df.saveToEs(esCfg.get("index") + "/" + esCfg.get("index_type"), this.esCfg)
  }
}
