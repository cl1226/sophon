package com.scistor.compute.output.batch

import java.io.{InputStream, PipedInputStream, PipedOutputStream}
import java.nio.charset.StandardCharsets
import java.sql.{Connection, DriverManager, SQLException}

import com.scistor.compute.apis.BaseOutput
import com.scistor.compute.model.spark.{SinkAttribute, SourceAttribute}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.postgresql.copy.CopyManager
import org.postgresql.core.BaseConnection

class Postgresql extends BaseOutput {

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

  def copyIn(data: Array[Row], sink: SinkAttribute): Long = {
    var conn: Connection = null
    try {
      println(s"postgresql url: ${sink.sink_connection_url}")
      Class.forName("org.postgresql.Driver")
      conn = DriverManager.getConnection(sink.sink_connection_url, sink.sink_connection_username, sink.sink_connection_password)
      val copyManager = new CopyManager(conn.asInstanceOf[BaseConnection])
      val tableName = sink.sinknamespace.split("\\.")(3)
      val cmd = s"COPY $tableName from STDIN DELIMITER ','"
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
    println("copy into postgresql...")
    df.rdd.mapPartitions(x => {
      copyIn(x.toArray, sinkAttribute)
      x
    }).count()
  }

}
