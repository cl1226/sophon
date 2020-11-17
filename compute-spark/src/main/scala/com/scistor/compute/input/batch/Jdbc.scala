package com.scistor.compute.input.batch

import java.util
import java.util.Properties

import com.scistor.compute.apis.BaseStaticInput
import com.scistor.compute.model.remote.TransStepDTO
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._

class Jdbc extends BaseStaticInput {

  var config: TransStepDTO = _

  var extraProp: Properties = new Properties

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
    if (!attrs.containsKey("connectUrl")) {
      (false, s"please specify [connectUrl] in ${attrs.getOrElse("dataSourceType", "")} as a non-empty string")
    } else {
      (true, "")
    }
  }

  def initProp(spark: SparkSession, driver: String): Tuple2[Properties, Array[String]] = {
    val attrs = config.getStepAttributes
    val definedProps = attrs.get("properties").asInstanceOf[util.Map[String, AnyRef]]
    val prop = new Properties()
    prop.setProperty("driver", driver)
    prop.setProperty("user", definedProps.get("user").toString)
    prop.setProperty("password", definedProps.get("password").toString)

    (prop, new Array[String](0))
  }

  def jdbcReader(sparkSession: SparkSession, driver: String): Dataset[Row] = {
    val strategy = config.getStrategy

    if (strategy != null) {
      println(s"[INFO] 任务 <${config.getName}> properties: ")
      println(s"\t执行模式=${strategy.getRunMode}")
      println(s"\t时间单位=${strategy.getTimeUnit}")
    }

    val attrs = config.getStepAttributes

    println(s"[INFO] 输入数据源 <${config.getStepType}> properties: ")
    attrs.foreach(entry => {
      val (key, value) = entry
      println("\t" + key + " = " + value)
    })

    var dataframe: Dataset[Row] = null
    val tuple = initProp(sparkSession, driver)
    if (tuple._2 != null && tuple._2.length > 0) {
      println(s"[INFO] 输入数据源 <${config.getStepType}> partitioned rules: ")
      tuple._2.map(x => {
        println(s"\t$x")
      })
      dataframe = sparkSession.read.jdbc(attrs.get("connectUrl").toString, attrs.get("source").toString, tuple._2, tuple._1)
    } else {
      dataframe = sparkSession.read.jdbc(attrs.get("connectUrl").toString, attrs.get("source").toString, tuple._1)
    }
    dataframe
  }

  override def getDataset(spark: SparkSession): Dataset[Row] = {
    jdbcReader(spark, "com.mysql.jdbc.Driver")
  }
}
