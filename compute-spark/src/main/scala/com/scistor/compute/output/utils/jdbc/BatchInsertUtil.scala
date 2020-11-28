package com.scistor.compute.output.utils.jdbc

import java.sql.{Date, Timestamp}
import java.util.Properties

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{BooleanType, ByteType, DateType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, TimestampType}

object BatchInsertUtil {
  def saveDFtoDBUsePool(prop: Properties, tableName: String, resultDateFrame: DataFrame): Unit = {
    val colNumbers = resultDateFrame.columns.length
    val sql = getInsertSql(tableName, resultDateFrame.columns)
    val columnDataTypes = resultDateFrame.schema.fields.map(_.dataType)

    resultDateFrame.foreachPartition(partitionRecords => {
      val conn = DBPoolManager.getDBPoolManager(prop).getConnection
      val prepareStatement = conn.prepareStatement(sql)
      val metaData = conn.getMetaData.getColumns(null, "%", tableName, "%")
      try {
        conn.setAutoCommit(false)
        partitionRecords.foreach(record => {
          for (i <- 1 to colNumbers) {
            val value = record.get(i - 1)
            val dateType = columnDataTypes(i - 1)
            if (value != null) {
              prepareStatement.setString(i, value.toString)
              dateType match {
                case _: ByteType => prepareStatement.setInt(i, record.getAs[Int](i - 1))
                case _: ShortType => prepareStatement.setInt(i, record.getAs[Int](i - 1))
                case _: IntegerType => prepareStatement.setInt(i, record.getAs[Int](i - 1))
                case _: LongType => prepareStatement.setLong(i, record.getAs[Long](i - 1))
                case _: BooleanType => prepareStatement.setBoolean(i, record.getAs[Boolean](i - 1))
                case _: FloatType => prepareStatement.setFloat(i, record.getAs[Float](i - 1))
                case _: DoubleType => prepareStatement.setDouble(i, record.getAs[Double](i - 1))
                case _: StringType => prepareStatement.setString(i, record.getAs[String](i - 1))
                case _: TimestampType => prepareStatement.setTimestamp(i, record.getAs[Timestamp](i - 1))
                case _: DateType => prepareStatement.setDate(i, record.getAs[Date](i - 1))
                case _ => throw new RuntimeException("nonsupport $ {dateType} !!!")
              }
            } else {
              metaData.absolute(i)
              prepareStatement.setNull(i, metaData.getInt("DATA_TYPE"))
            }
          }
          prepareStatement.addBatch()
        })
        prepareStatement.executeBatch()
        conn.commit()
      } catch {
        case e: Exception => println(s"@@ saveDFtoDBUsePool ${e.getMessage}")
      } finally {
        prepareStatement.close()
        conn.close()
      }
    })

  }

  def getInsertSql(tableName: String, cols: Array[String]): String = {
    val colNumbers = cols.length
    var sqlStr = "insert into " + tableName + "("
    for (i <- 1 to colNumbers) {
      sqlStr += cols(i - 1)
      if (i != colNumbers) {
        sqlStr += ","
      }
    }
    sqlStr += ") values("
    for (i <- 1 to colNumbers) {
      sqlStr += "?"
      if (i != colNumbers) {
        sqlStr += ","
      }
    }
    sqlStr += ") "
    sqlStr.substring(0, sqlStr.length - 1)
  }
}
