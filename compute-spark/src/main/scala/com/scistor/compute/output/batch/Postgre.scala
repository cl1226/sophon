package com.scistor.compute.output.batch

import java.io.{InputStream, PipedInputStream, PipedOutputStream}
import java.nio.charset.StandardCharsets
import java.sql.{Connection, DriverManager, SQLException}
import java.util
import java.util.Properties

import com.scistor.compute.apis.BaseOutput
import com.scistor.compute.model.remote.TransStepDTO
import com.scistor.compute.output.utils.jdbc.BatchInsertUtil
import org.apache.spark.sql.{Dataset, Row}
import org.postgresql.copy.CopyManager
import org.postgresql.core.BaseConnection

import scala.collection.JavaConversions._

class Postgre extends BaseOutput {

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
    (true, "")
  }

  def genPipedInputStream(arr: Array[Row]): InputStream = {
    val out = new PipedOutputStream
    (new Thread(){
      override def run {
        println("input data has " + arr.length + " rows")
        if (arr.length != 0) {
          val rowcount = arr.length;
          val columncount = arr(0).length
          println("input data has " + columncount + " columns")
          for (i <- 0 to rowcount-1; j <- 0 to columncount-1) {
            val separator = if (j == columncount-1) "\r\n" else ","
            val bytes = s"${arr(i)(j)}$separator".getBytes(StandardCharsets.UTF_8)
            out.write(bytes)
          }
        }
        out.close()
        println("PipedOutputStream closed")
      }
    }).start()
    val in = new PipedInputStream
    in.connect(out)
    in
  }

  def copyIn(data: Array[Row], str: String): Long = {
    val attrs = config.getStepAttributes
    var conn: Connection = null
    val definedProps = attrs.get("properties").asInstanceOf[util.Map[String, AnyRef]]
    try {
      println(s"postgresql url: ${attrs.get("connectUrl").toString}")

      val prop = new java.util.Properties
      for ((k, v) <- definedProps) {
        prop.setProperty(k, v.toString)
      }
      prop.setProperty("driver", "org.postgresql.Driver")
      conn = DriverManager.getConnection(attrs.get("connectUrl").toString, prop)

      val copyManager = new CopyManager(conn.asInstanceOf[BaseConnection])
      val tableName = attrs.get("source").toString
      val cmd = s"COPY $tableName ($str) from STDIN DELIMITER AS '&^&', null '', ignore_extra_data 'true', EOL '#^#', compatible_illegal_chars 'true'"
      println(s"copy cmd: $cmd")
      val count = copyManager.copyIn(cmd, genPipedInputStream(data))
      count
    } catch {
      case ex: SQLException => ex.printStackTrace(); 0
    } finally {
      try {
        if (conn != null) conn.close()
      } catch {
        case ex: SQLException => println(ex.getMessage())
      }
    }
  }

  override def process(df: Dataset[Row]): Unit = {
    val attrs = config.getStepAttributes
    println(s"[INFO] 输出数据源 <${config.getStepType}> properties: ")
    attrs.foreach(entry => {
      val (key, value) = entry
      println("\t" + key + " = " + value)
    })
    val definedProps = attrs.get("properties").asInstanceOf[util.Map[String, AnyRef]]
    val writeProps = attrs.get("write").asInstanceOf[util.Map[String, AnyRef]]
    val saveType = writeProps.getOrDefault("saveType", "jdbc")

    saveType match {
      case "jdbc" => {
        val saveMode = writeProps.getOrDefault("saveMode", "append").toString
        val prop = new Properties
        prop.setProperty("driver", "org.postgresql.Driver")
        for ((k, v) <- definedProps) {
          prop.setProperty(k, v.toString)
        }
        writeProps.foreach(entry => {
          val (key, value) = entry
          println("\t" + key + "=" + value)
        })

        df.write.mode(saveMode).jdbc(attrs.get("connectUrl").toString, attrs.get("source").toString, prop)
      }
      case "insert" => {
        val tableName = config.getStepAttributes.get("source").toString
        val prop: Properties = new Properties
        prop.setProperty("dialect", attrs.getOrElse("connectUrl", "").toString)
        prop.setProperty("driver", "org.postgresql.Driver")
        prop.setProperty("user", definedProps.getOrElse("user", "").toString)
        prop.setProperty("password", definedProps.getOrElse("password", "").toString)
        prop.setProperty("batchSize", definedProps.getOrElse("batchSize", "5000").toString)
        BatchInsertUtil.saveDFtoDBUsePool(prop, tableName, df)
      }
      case "copy" => {
        println("copy into gaussdb...")
        val columns: StringBuilder = new StringBuilder
        df.schema.foreach(col => {
          columns.append(s""""${col.name}"""").append(",")
        })
        val str = columns.toString().substring(0, columns.toString().length - 1)
        df.foreachPartition(part => {
          copyIn(part.toArray, str)
        })
      }
    }
  }

}
