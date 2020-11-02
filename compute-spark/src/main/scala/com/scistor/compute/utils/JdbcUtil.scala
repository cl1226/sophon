package com.scistor.compute.utils

import java.util
import java.util.{Calendar, Properties}

import com.scistor.compute.model.remote.ConnectConfig
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, LongType, StringType, StructField, StructType}

class JdbcUtil(sparkSession: SparkSession, connectConfig: ConnectConfig) extends Serializable {

  final val INPITTAB = "t_output_analysis"

  final val AGGTAB = "t_output_agg"

  val properties = new Properties()

  properties.setProperty("url", connectConfig.getConnection_url)
  properties.setProperty("user", connectConfig.getUser_name)
  properties.setProperty("password", connectConfig.getPassword)

  def writeDataCount(output: String, input: String, taskname: String): Unit = {
    if(!output.equals("0") || !input.equals("0")){
      val schema = StructType(
        List(
          StructField("output", LongType, true),
          StructField("input", LongType, true),
          StructField("time", StringType, true),
          StructField("taskname", StringType, true),
          StructField("jobuser", StringType, true)
        )
      )

      val format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
      val rows = new util.ArrayList[Row]()
      rows.add(
        Row(
          output.toLong,
          input.toLong,
          format.format(Calendar.getInstance().getTime),
          taskname,
          taskname.substring(0, if (taskname.lastIndexOf("_") >= 0) taskname.lastIndexOf("_") else taskname.length)
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
}
