package org.apache.spark.sql

import org.apache.spark.sql.types.StructType

object SparkSchemaUtil extends Serializable {

  def parseStructType(json: String): StructType = {
    StructType.fromString(json)
  }
}
