package com.scistor.compute.output.batch

import java.io.{InputStream, PipedInputStream, PipedOutputStream}
import java.nio.charset.StandardCharsets
import java.sql.{Connection, DriverManager, SQLException}

import com.huawei.gauss200.jdbc.copy.CopyManager
import com.huawei.gauss200.jdbc.core.BaseConnection
import com.scistor.compute.apis.BaseOutput
import com.scistor.compute.model.spark.SinkAttribute
import org.apache.spark.sql.{DataFrame, Row, functions}

class Gaussdb extends BaseOutput {

  var sinkAttribute: SinkAttribute = _

  /**
   * Set SinkAttribute.
   **/
  override def setSink(sink: SinkAttribute): Unit = {
    this.sinkAttribute = sink
  }

  /**
   * get SinkAttribute.
   **/
  override def getSink(): SinkAttribute = {
    this.sinkAttribute
  }

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
          val begin = System.currentTimeMillis()
          for (i <- 0 to rowcount-1; j <- 0 to columncount-1) {
            out.write((arr(i)(j) + (if (j == columncount-1) "#^#" else "&^&")).getBytes(StandardCharsets.UTF_8))
          }
          val end = System.currentTimeMillis()
          println(s"write into pipedinputstream cost time: ${end-begin}")
        }
        out.close()
        println("PipedOutputStream closed")
      }
    }).start()
    val in = new PipedInputStream
    in.connect(out)
    in
  }

  def copyIn(data: Array[Row], sink: SinkAttribute, str: String): Long = {
    var conn: Connection = null
    try {
      println(s"gaussdb url: ${sink.sink_connection_url}")
      Class.forName("com.huawei.gauss200.jdbc.Driver")
      conn = DriverManager.getConnection(sink.sink_connection_url, sink.sink_connection_username, sink.sink_connection_password)
      val copyManager = new CopyManager(conn.asInstanceOf[BaseConnection])
      val tableName = sink.sinknamespace.split("\\.")(3)
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

  def process(df: DataFrame): Unit = {
    val params = sinkAttribute.parameters
    val saveType = params.getOrDefault("saveType", "jdbc")

    saveType match {
      case "jdbc" => {
        val saveMode = params.getOrDefault("saveMode", "append")
        val prop = new java.util.Properties
        prop.setProperty("driver", "com.huawei.gauss200.jdbc.Driver")
        prop.setProperty("user", sinkAttribute.sink_connection_username)
        prop.setProperty("password", sinkAttribute.sink_connection_password)

        df.write.mode(saveMode).jdbc(sinkAttribute.sink_connection_url, sinkAttribute.tableName, prop)
      }
      case "copy" => {
        println("copy into gaussdb...")
        val columns: StringBuilder = new StringBuilder
        df.schema.foreach(col => {
          columns.append(s""""${col.name}"""").append(",")
        })
        val str = columns.toString().substring(0, columns.toString().length - 1)
        df.rdd.mapPartitions(x => {
          copyIn(x.toArray, sinkAttribute, str)
          x
        }).count()
      }
    }

  }

}
