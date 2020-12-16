package com.scistor.compute.input.batch

import java.text.{DateFormat, SimpleDateFormat}
import java.util
import java.util.{Calendar, Properties}

import com.scistor.compute.utils.{JdbcUtil, SparkInfoTransfer}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

class Gaussdb extends Jdbc {

  override def getDataset(spark: SparkSession): Dataset[Row] = {
    jdbcReader(spark, "com.huawei.gauss200.jdbc.Driver")
  }

  override def initProp(spark: SparkSession, driver: String): (Properties, Array[String]) = {
    val attrs = config.getStepAttributes
    val prop = new Properties()
    val definedProps = attrs.get("properties").asInstanceOf[util.Map[String, AnyRef]]
    for ((k, v) <- definedProps) {
      prop.setProperty(k, v.toString)
    }
    prop.setProperty("driver", driver)

    // 读取方式：全量(full)/增量(increment)
    val read = attrs.get("read").asInstanceOf[util.Map[String, AnyRef]]
    val partColumnName = definedProps.getOrElse("partColumnName", "")
    var predicates: Array[String] = null
    read.getOrDefault("type", "full").toString match {
      case "full" => {
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
      case "increment" => {
        val incrementColumn = read.getOrElse("jdbcTimestampColumn", "").toString
        incrementColumn match {
          case "" => {
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
                  predicates = arr.map(i=>{s"SHA1($partColumnName)%$numPartitions = $i"}).toArray
                }
                (prop, predicates)
              }
            }
          }
          case _ => {
            val format: DateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            val now = Calendar.getInstance()
            // 获取最近一次读取的增量时间记录
            val lastReadTimestamp: String = new JdbcUtil(spark, SparkInfoTransfer.jobInfo.getMysqlConfig).queryIncrementalTime(
              config.getId,
              config.getName,
              config.getStepId
            )
            val startTime = format.parse(lastReadTimestamp).getTime
            val precision = now.getTime.getTime - startTime
            val totalInterval = precision
            val numPartitions: Int = Integer.valueOf(definedProps.getOrElse("numPartitions", "1").toString)
            val tuples = ArrayBuffer[(String, String)]()
            val unitInterval = totalInterval / numPartitions
            for (i <- 0 until numPartitions ) {
              val start = format.format(startTime + i * unitInterval)
              val end  = format.format(startTime + (i + 1) * unitInterval)
              tuples.+= ((start, end))
            }
            predicates = tuples.map(elem => {
              s"cast($incrementColumn as timestamp) >= timestamp '${elem._1}' and cast($incrementColumn as timestamp) < timestamp '${elem._2}'"
            }).toArray
            prop.setProperty("now", format.format(now.getTime))

            (prop, predicates)
          }
        }
      }
    }
  }
}
