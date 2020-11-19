package com.scistor.compute.input.batch

import java.util

import com.scistor.compute.apis.BaseStaticInput
import com.scistor.compute.model.remote.TransStepDTO
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._

class Es extends BaseStaticInput {

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
    attrs.containsKey("host") && attrs.containsKey("index") && attrs.get("host").toString.size > 0 match {
      case true => {
        // TODO CHECK hosts
        (true, "")
      }
      case false => (false, "please specify [host] as a non-empty string list")
    }
  }

  /**
   * Get DataFrame from this Static Input.
   **/
  override def getDataset(spark: SparkSession): Dataset[Row] = {
//    val index = source.sourcenamespace.split("\\.")(2) //es.es.user.user.*.*.*
//    val types = source.sourcenamespace.split("\\.")(3) //es.es.user.user.*.*.*
    val attrs = config.getStepAttributes
    val index = attrs.getOrElse("index", "").toString
    val types = attrs.getOrElse("type", "").toString

    val host = attrs.get("host").toString
    val port = attrs.get("port").toString
    val esOptions = new util.HashMap[String, String]
    esOptions.put("es.nodes", host)
    esOptions.put("es.port", port)
    esOptions.put("es.index.read.missing.as.empty","true")
    esOptions.put("es.nodes.wan.only","true")
    esOptions.put("es.scroll.size", "10000")
    esOptions.put("es.field.read.empty.as.null","true")// es 7.2.x
    esOptions.put("es.index.read.missing.as.empty","true")// es 7.2.x
    esOptions.put("es.mapping.date.rich", "false") //不解析date直接返回string

    val extraProps = attrs.get("properties").asInstanceOf[util.Map[String, AnyRef]]
    if (extraProps != null && extraProps.containsKey("user")) {
      esOptions.put("es.net.http.auth.user", extraProps.get("user").toString) //访问es的用户名
    }
    if (extraProps != null && extraProps.containsKey("password")) {
      esOptions.put("es.net.http.auth.pass", extraProps.get("password").toString) //访问es的用户名
    }

    println("[INFO] Input ElasticSearch Params:")
    esOptions.foreach(entry => {
      val (key, value) = entry
      println("\t" + key + " = " + value)
    })

    val reader = spark.read.format("org.elasticsearch.spark.sql")
      val res = reader.options(esOptions).load(index)
    res
  }
}
