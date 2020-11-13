package com.scistor.compute.input.batch

import java.text.{DateFormat, SimpleDateFormat}
import java.util
import java.util.{Calendar, Properties}

import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions.mapAsScalaMap
import scala.collection.mutable.ArrayBuffer

class Oracle extends Jdbc {

  override def getDataset(spark: SparkSession): Dataset[Row] = {
    jdbcReader(spark, "oracle.jdbc.driver.OracleDriver")
  }

  override def initProp(spark: SparkSession, driver: String): (Properties, Array[String]) = {
    val attrs = config.getStepAttributes
    val prop = new Properties()
    val definedProps = attrs.get("properties").asInstanceOf[util.Map[String, AnyRef]]
    for ((k, v) <- definedProps) {
      prop.setProperty(k, v.toString)
    }
    prop.setProperty("driver", driver)

    val strategy = config.getStrategy
    strategy.getRunMode match {
      case "single" => {
        // 单次执行: 根据页面提供的分区字段并行读
        val partColumnName = definedProps.getOrElse("partColumnName", "")
        partColumnName match {
          case "" => (prop, new Array[String](0))
          case _ => {
            val numPartitions: Int = Integer.valueOf(definedProps.getOrElse("numPartitions", "1").toString)
            var predicates: Array[String] = null
            // 取模的方式划分分区, 所以分区字段的选择最好是均匀分布的, 分区的效果比较好
            if (!partColumnName.equals("")) {
              val arr = ArrayBuffer[Int]()
              for(i <- 0 until numPartitions){
                arr.append(i)
              }
              predicates = arr.map(i=>{s"$partColumnName%$numPartitions = $i"}).toArray
            }
            (prop, predicates)
          }
        }
      }
      case "cycle" => {
        val partColumnName = definedProps.getOrElse("partColumnName", "")
        partColumnName match {
          case "" => (prop, new Array[String](0))
          case _ => {
            val numPartitions: Int = Integer.valueOf(definedProps.getOrElse("numPartitions", "1").toString)

            var precision = 0
            val totalLen = 1
            var predicates: Array[String] = null

            strategy.getTimeUnit match {
              case "HOUR" => precision = 60 * 60 * 1000
              case "DAILY" => precision = 24 * 60 * 60 * 1000
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
              s"cast($partColumnName as datetime) >= '${elem._1}' and cast($partColumnName as datetime) < '${elem._2}'"
            }).toArray
            (prop, predicates)
          }
        }
      }
    }
  }
}
