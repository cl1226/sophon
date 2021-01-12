package com.scistor.compute.output.batch

import com.scistor.compute.SparkJobStarter.session
import com.scistor.compute.apis.BaseOutput
import com.scistor.compute.model.remote.TransStepDTO
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{Dataset, Row, SaveMode}

import java.util
import java.util.Properties
import scala.collection.JavaConversions._

class Hive extends BaseOutput {

  var config: TransStepDTO = _

  /**
   * Set Config.
   * */
  override def setConfig(config: TransStepDTO): Unit = {
    this.config = config
  }

  /**
   * Get Config.
   * */
  override def getConfig(): TransStepDTO = config

  override def process(df: Dataset[Row]): Unit = {
    val attrs = config.getStepAttributes
    val prop = new Properties()

    val writeProps = attrs.get("write").asInstanceOf[util.Map[String, AnyRef]]
    val definedProps = attrs.get("properties").asInstanceOf[util.Map[String, AnyRef]]
    for ((k, v) <- definedProps) {
      prop.setProperty(k, v.toString)
    }

    println(s"[INFO] 输出数据源 [${config.getStepType}] properties: ")
    attrs.foreach(entry => {
      val (key, value) = entry
      println("\t" + key + " = " + value)
    })
    prop.foreach(entry => {
      val (key, value) = entry
      println("\t" + key + " = " + value)
    })
    writeProps.foreach(entry => {
      val (key, value) = entry
      println("\t" + key + "=" + value)
    })

    val partitionColumn = definedProps.getOrDefault("partitionColumn", "").toString
    val table = attrs.get("source").toString

//    val hiveContext = new HiveContext(session.sparkContext)
    session.sql(s"use ${attrs.get("dataBaseName").toString}")
//    partitionColumn.equals("") match {
//      case true => session.sql(s"insert into ${table} (${hiveColumn.substring(1)})" +
//        s" select ${dfColumn.substring(1)} from ${config.getStepFrom}")
//      case false => session.sql(s"insert into ${table} (${hiveColumn.substring(1)}) partition($partitionColumn) " +
//        s" select ${dfColumn.substring(1)} from ${config.getStepFrom}")
//    }

    val saveMode = writeProps.getOrDefault("saveMode", "append").toString
    saveMode match {
      case "append" => {
        var finalDF = df
        val hiveSchemaDF = session.sql(s"desc $table").collect()
        hiveSchemaDF.foreach(row => {
          val col = row.getAs[String]("col_name")
          if (!finalDF.columns.contains(col)) finalDF = finalDF.withColumn(col, lit(null))
        })
        partitionColumn match {
          case "" => finalDF.write.mode(SaveMode.Append).format("hive").insertInto(table)
          case _ => finalDF.write.mode(SaveMode.Append).format("hive").partitionBy(partitionColumn).insertInto(table)
        }
      }
      case "overwrite" => {
        partitionColumn match {
          case "" => df.write.mode(SaveMode.Overwrite).format("hive").saveAsTable(table)
          case _ => df.write.mode(SaveMode.Overwrite).format("hive").partitionBy(partitionColumn).saveAsTable(table)
        }
      }
      case "errorifexists" => {
        partitionColumn match {
          case "" => df.write.mode(SaveMode.ErrorIfExists).format("hive").saveAsTable(table)
          case _ => df.write.mode(SaveMode.ErrorIfExists).format("hive").partitionBy(partitionColumn).saveAsTable(table)
        }
      }
      case "ignore" => {
        partitionColumn match {
          case "" => df.write.mode(SaveMode.Ignore).format("hive").saveAsTable(table)
          case _ => df.write.mode(SaveMode.Ignore).format("hive").partitionBy(partitionColumn).saveAsTable(table)
        }
      }
    }
  }

  /**
   * Return true and empty string if config is valid, return false and error message if config is invalid.
   */
  override def validate(): (Boolean, String) = (true, "")
}
