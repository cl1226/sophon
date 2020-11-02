package com.scistor.compute.input.batch

import java.text.{DateFormat, SimpleDateFormat}
import java.util
import java.util.{Calendar, Properties}

import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

class Gbase extends Jdbc {

  override def getDataset(spark: SparkSession): Dataset[Row] = {
    jdbcReader(spark, "com.gbase.jdbc.Driver")
  }

  override def initProp(driver: String): (Properties, Array[String]) = {
    val attrs = config.getStepAttributes
    val definedProps = attrs.get("properties").asInstanceOf[util.Map[String, AnyRef]]
    val prop = new Properties()
    prop.setProperty("driver", driver)
    prop.setProperty("user", definedProps.get("user").toString)
    prop.setProperty("password", definedProps.get("password").toString)

    val partColumnName = definedProps.getOrDefault("partColumnName", "").toString
    partColumnName match {
      case "" => (prop, new Array[String](0))
      case _ => {
        val numPartitions: Int = definedProps.getOrDefault("numPartitions", "1").asInstanceOf[Int]

        var precision = 0
        val totalLen = 1
        val timeunit = extraProp.getProperty("timeunit")
        var predicates: Array[String] = null
        if (timeunit != null) {
          // 根据定时任务时间划分取数范围
          timeunit match {
            case "hour" => precision = 60 * 60 * 1000
            case "daily" => precision = 24 * 60 * 60 * 1000
          }

          val format: DateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
          val totalInterval = totalLen * precision
          val now = Calendar.getInstance()
          val startTime = now.getTime.getTime - totalInterval
          val unitInterval = totalInterval / numPartitions
          val tuples = ArrayBuffer[(String, String)]()
          for (i <- 0 until numPartitions ) {
            val start = format.format(startTime + i * unitInterval)
            val end  = format.format(startTime + (i + 1) * unitInterval)
            tuples.+= ((start, end))
          }

          predicates = tuples.map(elem => {
            s"$partColumnName >= '${elem._1}' and $partColumnName < '${elem._2}'"
          }).toArray
        } else {
          // 取模的方式划分分区
          if (!partColumnName.equals("")) {
            val arr = ArrayBuffer[Int]()
            for(i <- 0 until numPartitions){
              arr.append(i)
            }
            predicates = arr.map(i=>{s"$partColumnName%$numPartitions = $i"}).toArray
          }
        }
        (prop, predicates)
      }
    }

  }
}
