package com.scistor.compute.utils

import com.alibaba.fastjson.JSON
import com.scistor.compute.model.spark.ProjectInfo
import com.scistor.compute.utils.JobInfoTransfer.jedis
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._

object CommonUtil {

  def writeSimpleData(df: DataFrame, job: ProjectInfo, redisKey: String): Unit = {
    val arrayData = df.take(job.getShowNumbers).map(_.getValuesMap(df.schema.map(_.name)).asInstanceOf[Map[String, Any]].mapValues(cell => {
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

    if (arrayData.size > 0) jedis.set(redisKey, JSON.toJSON(arrayData).toString)
  }

}
