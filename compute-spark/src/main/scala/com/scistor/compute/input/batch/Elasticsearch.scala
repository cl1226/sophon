package com.scistor.compute.input.batch

import java.util

import com.scistor.compute.apis.BaseStaticInput
import com.scistor.compute.model.spark.SourceAttribute
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._

class Elasticsearch extends BaseStaticInput {

  var source:SourceAttribute = _

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
//    val index = source.sourcenamespace.split("\\.")(2) //es.es.user.user.*.*.*
//    val types = source.sourcenamespace.split("\\.")(3) //es.es.user.user.*.*.*
    val index = source.databaseName
    val types = source.table_name

    val port = source.connection_url.split(",")(0).split(":")(1)
    val esOptions = new util.HashMap[String, String]
    esOptions.put("es.nodes", source.connection_url.split(":")(0)) //localhost
    esOptions.put("es.port", port)
    esOptions.put("es.index.read.missing.as.empty","true")
    esOptions.put("es.nodes.wan.only","true")
    esOptions.put("es.scroll.size", "10000")
    esOptions.put("es.field.read.empty.as.null","true")// es 7.2.x
    esOptions.put("es.index.read.missing.as.empty","true")// es 7.2.x
    esOptions.put("es.mapping.date.rich", "false") //不解析date直接返回string
    if(StringUtils.isNoneBlank(source.username)) {
      esOptions.put("es.net.http.auth.user", source.username) //访问es的用户名
    }
    if(StringUtils.isNoneBlank(source.password)) {
      esOptions.put("es.net.http.auth.pass", source.password) ////访问es的密码
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
