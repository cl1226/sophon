package com.scistor.compute.transform

import com.scistor.compute.apis.BaseTransform
import com.scistor.compute.model.remote.TransStepDTO
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class Script extends BaseTransform {

  var config: TransStepDTO =_

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
    val sql = config.getStepAttributes.get("codeBlock").toString
    spark.sql(sql)
  }

  /**
   * Return true and empty string if config is valid, return false and error message if config is invalid.
   */
  override def validate(): (Boolean, String) = {
    val sql = config.getStepAttributes.get("codeBlock").toString
    if (StringUtils.isNotBlank(sql)) {
      val sparkSession = SparkSession.builder.getOrCreate
      val logicalPlan = sparkSession.sessionState.sqlParser.parsePlan(sql)
      if (!logicalPlan.resolved) {
        val logicPlanStr = logicalPlan.toString
        logicPlanStr.toLowerCase.contains("unresolvedrelation") match {
          case true => (true, "")
          case false => {
            val msg = "config[sql] cannot be passed through sql parser, sql[" + sql + "], logicPlan: \n" + logicPlanStr
            (false, msg)
          }
        }
      } else {
        (true, "")
      }
    } else {
      (false, "please specify [script] as String")
    }

  }
}
