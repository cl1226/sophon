package com.scistor.compute.output.batch

import java.util

import com.scistor.compute.apis.BaseOutput
import com.scistor.compute.model.remote.TransStepDTO
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.elasticsearch.spark.sql._

import scala.collection.JavaConversions._

class Es extends BaseOutput {

  var esCfg: Map[String, String] = Map()
  val esPrefix = "es."

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
    val attrs = config.getStepAttributes
    attrs.containsKey("host") && attrs.containsKey("index") && attrs.get("host").toString.length > 0 match {
      case true => {
        // TODO CHECK hosts
        (true, "")
      }
      case false => (false, "please specify [host] as a non-empty string list")
    }
  }

  /**
   * Prepare before running, do things like set config default value, add broadcast variable, accumulator.
   */
  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val attrs = config.getStepAttributes

    esCfg += ("index" -> attrs.get("index").toString)
    esCfg += ("index_type" -> attrs.getOrElse("type", "").toString)
    esCfg += ("index_time_format" -> "yyyy.MM.dd")
    esCfg += ("es.nodes" -> attrs.get("host").toString)

    val extraProps = attrs.get("properties").asInstanceOf[util.Map[String, AnyRef]]
    if (extraProps.containsKey("user")) {
      esCfg += ("es.net.http.auth.user" -> extraProps.get("user").toString)
    }
    if (extraProps.containsKey("password")) {
      esCfg += ("es.net.http.auth.user" -> extraProps.get("password").toString)
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
