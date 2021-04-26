package com.scistor.compute.transform

import com.scistor.compute.apis.BaseTransform
import com.scistor.compute.model.remote.{OperatorImplementMethod, TransStepDTO}
import com.scistor.compute.model.spark.InvokeInfo
import com.scistor.compute.utils.CommonUtil
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.ComputeProcess.{pipeLineProcess, processDynamicCode}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import java.util
import scala.collection.JavaConversions.mapAsScalaMap
import scala.collection.JavaConverters.asScalaBufferConverter

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
    var frame = df

    println(s"[INFO] 转换算子 [${config.getStepType}] properties: ")
    attrs.foreach(entry => {
      val (key, value) = entry
      println("\t" + key + " = " + value)
    })

    OperatorImplementMethod.get(attrs.get("implementMethod").toString) match {
      case OperatorImplementMethod.ScriptSql => {
        val sql = attrs.get("codeBlock").toString
        frame = spark.sql(sql)
      }
      case OperatorImplementMethod.ScriptJava | OperatorImplementMethod.ScriptScala => {
        val invokeInfo = new InvokeInfo("", attrs.get("methodName").toString, CommonUtil.portalField2ComputeField(config.getInputFields))
        invokeInfo.setCode(attrs.get("codeBlock").toString)
        config.setStepType(attrs.get("implementMethod").toString)
        frame = processDynamicCode(spark, df, invokeInfo, config)
      }
      // execute python/shell
      case OperatorImplementMethod.CommandPython | OperatorImplementMethod.CommandShell => {
        val path = attrs.get("codeFileUrl").toString
        val fileName = path.substring(path.lastIndexOf("/") + 1, path.length)
        val commands = new util.ArrayList[String]()
        if(fileName.endsWith("py")) commands.add("python") else if(fileName.endsWith("sh")) commands.add("sh")
        commands.add(fileName)

        var index: Int = 0
        config.getInputFields.asScala.foreach(input => {
          frame = pipeLineProcess(spark, frame, input.getStreamFieldName, commands.toArray[String](Array[String]()), config.getOutputFields.get(index).getStreamFieldName)
          index = index + 1
        })
      }
      case _ => frame = df
    }

    frame
  }

  /**
   * Return true and empty string if config is valid, return false and error message if config is invalid.
   */
  override def validate(): (Boolean, String) = {
    val attrs = config.getStepAttributes
    val code = config.getStepAttributes.get("codeBlock").toString
    if (StringUtils.isBlank(code)) {
      (false, "please specify [script] as a non-empty string")
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
        case OperatorImplementMethod.ScriptJava | OperatorImplementMethod.ScriptScala => {
          if (!attrs.containsKey("methodName") || attrs.get("methodName").equals("")) {
            (false, s"please specify ${config.getStepType} [methodName] as string")
          } else {
            (true, "")
          }
        }
        case _ => (true, "")
      }
    }
  }
}
