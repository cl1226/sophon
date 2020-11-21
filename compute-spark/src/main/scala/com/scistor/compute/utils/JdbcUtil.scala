package com.scistor.compute.utils

import java.util
import java.util.{Calendar, Properties}

import com.scistor.compute.model.remote.ConnectConfig
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, LongType, StringType, StructField, StructType, TimestampType}

class JdbcUtil(sparkSession: SparkSession, connectConfig: ConnectConfig) extends Serializable {

  final val INPITTAB = "t_compute_trans_task_amount"

  final val AGGTAB = "t_output_agg"

  val properties = new Properties()

  properties.setProperty("url", connectConfig.getConnection_url)
  properties.setProperty("user", connectConfig.getUser_name)
  properties.setProperty("password", connectConfig.getPassword)
  properties.setProperty("driver", "com.mysql.cj.jdbc.Driver")

  def writeDataCount(output: String, input: String, taskname: String): Unit = {
    if(!output.equals("0") || !input.equals("0")){
      val schema = StructType(
        List(
          StructField("yarnid", StringType, true),
          StructField("input", LongType, true),
          StructField("output", LongType, true),
          StructField("time", StringType, true)
        )
      )

      val format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
      val rows = new util.ArrayList[Row]()
      rows.add(
        Row(
          taskname,
          input.toLong,
          output.toLong,
          format.format(Calendar.getInstance().getTime)
        )
      )

      val df = sparkSession.createDataFrame(rows, schema)
      df.write.mode(SaveMode.Append).jdbc(properties.getProperty("url"), INPITTAB, properties)
    }
  }

  def getMaxColumnValue(colName: String, tableName: String): String = {
    val df = sparkSession.sql(s"""select max($colName) as res from $tableName""")
    val rows = df.take(1)
    rows(0).getAs[String]("res")
  }

  def writeIncrementalTime(datetime: String, transId: String, yarnId: String, stepId: String): Unit = {
    val tableName = "t_compute_trans_incremental_timestamp"
    val schema = StructType(
      List(
        StructField("transId", StringType, true),
        StructField("yarnId", StringType, true),
        StructField("stepId", StringType, true),
        StructField("time", StringType, true)
      )
    )
    val rows = new util.ArrayList[Row]()
    rows.add(
      Row(
        transId,
        yarnId,
        stepId,
        datetime
      )
    )
    val df = sparkSession.createDataFrame(rows, schema)
    df.write.mode(SaveMode.Append).jdbc(properties.getProperty("url"), tableName, properties)
  }

  def queryIncrementalTime(transId: String, yarnId: String, stepId: String): String = {
    val query = s"select time from t_compute_trans_incremental_timestamp " +
      s"where transId='${transId}'" +
      s" and yarnId='${yarnId}'" +
      s" and stepId='${stepId}'" +
      s" order by time desc" +
      s" limit 1"

    val df = sparkSession.read
      .format("jdbc")
      .option("url", properties.getProperty("url"))
      .option("dbtable", s"(${query}) a")
      .option("user", properties.getProperty("user"))
      .option("password", properties.getProperty("password"))
      .load()
    if (df.count() > 0) {
      df.take(1)(0).getAs[String]("time")
    } else {
      "1970-01-01 00:00:00"
    }
  }
}
