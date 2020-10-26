package com.scistor.compute.utils

import java.util.List

import com.scistor.compute.model.spark.{DataType, SourceSchme}
import org.apache.spark.sql.types.{StructField, StructType}

import scala.collection.JavaConverters._

object SchemaUtil extends Serializable {

  def converterSchme2Spark(schmes: List[SourceSchme]): StructType ={
    new StructType(schmes.asScala.map(converterField(_)).toArray)
  }

  private def converterField(source_field: SourceSchme): StructField ={
    StructField(source_field.name, DataType.valueOf(source_field.`type`.toUpperCase()).getSparkDataType, source_field.nullable)
  }

  def parseStructType(json: String): StructType = {
//    StructType.fromString(json)
    new StructType()
  }
}
