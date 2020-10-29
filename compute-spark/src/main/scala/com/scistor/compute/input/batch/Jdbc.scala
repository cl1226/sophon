package com.scistor.compute.input.batch

import java.util.Properties

import com.scistor.compute.apis.BaseStaticInput
import com.scistor.compute.model.spark.{SinkAttribute, SourceAttribute}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class Jdbc extends BaseStaticInput {

  var config: SourceAttribute = _

  var extraProp: Properties = new Properties

  /**
   * Set SourceAttribute.
   **/
  override def setSource(source: SourceAttribute): Unit = {
    this.config = source
  }

  /**
   * get SourceAttribute.
   **/
  override def getSource(): SourceAttribute = {
    this.config
  }

  /**
   * Return true and empty string if config is valid, return false and error message if config is invalid.
   */
  override def validate(): (Boolean, String) = {
    (true, "")
  }

  def initProp(driver: String): Tuple2[Properties, Array[String]] = {
    val prop = new Properties()
    prop.setProperty("driver", driver)
    prop.setProperty("user", config.username)
    prop.setProperty("password", config.password)

    (prop, new Array[String](0))
  }

  def jdbcReader(sparkSession: SparkSession, driver: String): Dataset[Row] = {
    var dataframe: Dataset[Row] = null
    val tuple = initProp(driver)
    if (tuple._2 != null && tuple._2.length > 0) {
      tuple._2.map(x => {
        println(s"source predicates: $x")
      })
      dataframe = sparkSession.read.jdbc(config.connection_url, config.table_name, tuple._2, tuple._1)
    } else {
      dataframe = sparkSession.read.jdbc(config.connection_url, config.table_name, tuple._1)
    }

    dataframe
  }

  override def getDataset(spark: SparkSession): Dataset[Row] = {
    jdbcReader(spark, "com.mysql.jdbc.Driver")
  }
}
