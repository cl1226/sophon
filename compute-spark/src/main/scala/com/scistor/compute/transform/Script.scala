package com.scistor.compute.transform

import com.scistor.compute.apis.BaseTransform
import com.scistor.compute.model.remote.{OperatorImplementMethod, TransStepDTO}
import com.scistor.compute.model.spark.InvokeInfo
import com.scistor.compute.utils.CommonUtil
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.ComputeProcess.{pipeLineProcess, processDynamicCode}
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
    val attrs = config.getStepAttributes
    OperatorImplementMethod.get(attrs.get("implementMethod").toString) match {
      case OperatorImplementMethod.ScriptSql => {
        val sql = attrs.get("codeBlock").toString
        spark.sql(sql)
      }
      case OperatorImplementMethod.ScriptJava | OperatorImplementMethod.ScriptJava => {
        val invokeInfo = new InvokeInfo("", attrs.get("methodName").toString, CommonUtil.portalField2ComputeField(config.getInputFields))
        invokeInfo.setCode(attrs.get("codeBlock").toString)
        config.setStepType(attrs.get("implementMethod").toString)
        processDynamicCode(spark, df, invokeInfo, config)
      }
      case OperatorImplementMethod.CommandPython => {
        val frame = pipeLineProcess(spark, df, "python", Array[String](), "")
        frame
      }
      case OperatorImplementMethod.CommandShell => {
        val frame = pipeLineProcess(spark, df, "sh", Array[String](), "")
        frame
      }
      case _ => df
    }

  }

  /**
   * Return true and empty string if config is valid, return false and error message if config is invalid.
   */
  override def validate(): (Boolean, String) = {
    val attrs = config.getStepAttributes
    val code = config.getStepAttributes.get("codeBlock").toString
    if (StringUtils.isBlank(code)) {
      (false, "please specify [script] as string")
    } else {
      OperatorImplementMethod.get(attrs.get("implementMethod").toString) match {
        case OperatorImplementMethod.ScriptSql => {
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
        case _ => {
          if (!attrs.containsKey("methodName") || attrs.get("methodName").equals("")) {
            (false, s"please specify ${config.getStepType} [methodName] as string")
          } else {
            (true, "")
          }
        }
      }
    }
  }
}
