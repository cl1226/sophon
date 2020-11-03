package com.scistor.compute.utils

import java.util
import com.alibaba.fastjson.{JSON, JSONObject}
import com.scistor.compute.model.remote.StreamFieldDTO
import com.scistor.compute.model.spark.{ComputeField, DataType}
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._

object CommonUtil {

  def writeSimpleData(df: DataFrame, redisKey: String): Unit = {
    val arrayData = df.take(20).map(_.getValuesMap(df.schema.map(_.name)).asInstanceOf[Map[String, Any]].mapValues(cell => {
      val truncate = 150
      val str = cell match {
        case null => "null"
        case binary: Array[Byte] => binary.map("%02X".format(_)).mkString("[", " ", "]")
        case _ => cell.toString
      }
      if (truncate > 0 && str.length > truncate) {
        // do not show ellipses for strings shorter than 4 characters.
        if (truncate < 4) str.substring(0, truncate)
        else str.substring(0, truncate - 3) + "..."
      } else {
        str
      }
    }).asJava)

    if (arrayData.size > 0) SparkInfoTransfer.jedis.set(redisKey, JSON.toJSON(arrayData).toString)
  }

  def portalField2ComputeField(portalFields: util.List[StreamFieldDTO]): util.List[ComputeField] = {
    var computeFields = new util.ArrayList[ComputeField]()
    for (f <- portalFields.asScala) {
      val field = new ComputeField(f.getFieldName, DataType.valueOf(f.getFieldType.toUpperCase()), true)
      field.setConstant(f.getConstant)
      field.setConstantValue(f.getConstantValue)
      field.setStructFieldJson(buildStreamFieldNameJson(f))
      computeFields.add(field)
    }
    computeFields
  }

  def buildStreamFieldNameJson(field: StreamFieldDTO): String = {
    val json = new JSONObject()
    json.put("name", field.getStreamFieldName)
    json.put("type", field.getFieldType)
    json.put("nullable", true)
    json.put("metadata", {})
    json.toJSONString
  }

}
