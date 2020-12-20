package com.scistor.compute.transform

import com.scistor.compute.apis.BaseTransform
import com.scistor.compute.model.remote.TransStepDTO
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions.mapAsScalaMap

class Join extends BaseTransform {

  var config: TransStepDTO = _

  /**
   * Set Config.
   * */
  override def setConfig(config: TransStepDTO): Unit = {
    this.config = config
  }

  /**
   * Get Config.
   * */
  override def getConfig(): TransStepDTO = config

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {
    val attrs = config.getStepAttributes

    println(s"[INFO] 转换算子 [${config.getStepType}] properties: ")
    attrs.foreach(entry => {
      val (key, value) = entry
      println("\t" + key + " = " + value)
    })

    val sql = attrs.get("sql").toString
    val res = spark.sql(sql)
//    val sourceTableName = attrs.get("sourceTableName").toString
//    val targetTableName = attrs.get("targetTableName").toString
//    val joinField: Seq[String] = attrs.get("joinField").toString.split(",").toSeq
//    val joinType = attrs.getOrDefault("joinType", "inner").toString
//    val sourceDF = spark.read.table(sourceTableName)
//    val targetDF = spark.read.table(targetTableName)
//    val df = sourceDF.join(targetDF, joinField, joinType)
    res
  }

  /**
   * Return true and empty string if config is valid, return false and error message if config is invalid.
   */
  override def validate(): (Boolean, String) = {
    val attrs = config.getStepAttributes
    attrs.containsKey("sql") && attrs.get("sql").toString.length > 0 match {
      case true => {
        val code = attrs.get("sql").toString
        val sparkSession = SparkSession.builder.getOrCreate
        val logicalPlan = sparkSession.sessionState.sqlParser.parsePlan(code)
        if (!logicalPlan.resolved) {
          val logicPlanStr = logicalPlan.toString
          logicPlanStr.toLowerCase.contains("unresolvedrelation") match {
            case true => (true, "")
            case false => {
              val msg = "config[sql] cannot be passed through sql parser, sql[" + code + "], logicPlan: \n" + logicPlanStr
              (false, msg)

            }
          }
        } else {
          (true, "")
        }
      }
      case false => (false, "please specify [join sql] as a non-empty string")
    }
    //    if (!attrs.containsKey("sourceTableName")) {
//      (false, "please specify [sourceTableName] as a non-empty string")
//    } else if (!attrs.containsKey("targetTableName")) {
//      (false, "please specify [targetTableName] as a non-empty string")
//    } else if(!attrs.containsKey("joinField")) {
//      (false, "please specify [joinField] as a non-empty string")
//    } else {
//      (true, "")
//    }
  }
}
