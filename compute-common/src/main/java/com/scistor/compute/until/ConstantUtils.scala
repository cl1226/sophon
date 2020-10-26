package com.scistor.compute.until

import org.apache.spark.sql.types.{DataTypes, StructField}

object ConstantUtils {
  def castConstant(sfd: StructField, constant: String): Any = {
    sfd.dataType match {
      case DataTypes.StringType => constant
      case DataTypes.IntegerType => constant.toInt
      case DataTypes.DoubleType => constant.toDouble
      case DataTypes.LongType => constant.toLong
      case DataTypes.BooleanType => constant.toBoolean
      case DataTypes.ByteType => constant.toByte
      case DataTypes.FloatType => constant.toFloat
      case DataTypes.NullType => null
      case DataTypes.ShortType => constant.toShort
      case _ => constant
    }
  }
}
