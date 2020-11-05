package com.scistor.compute.transform

import com.scistor.compute.apis.BaseTransform
import com.scistor.compute.interfacex.{ComputeOperator, SparkProcessProxy}
import com.scistor.compute.model.remote.{OperatorImplementMethod, TransStepDTO}
import com.scistor.compute.model.spark.{InvokeInfo, UserDefineOperator}
import com.scistor.compute.until.ClassUtils
import com.scistor.compute.utils.CommonUtil
import org.apache.spark.sql.ComputeProcess.{computeOperatorProcess, computeSparkProcess, pipeLineProcess, processDynamicCode}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class UserDefinedTransform extends BaseTransform {

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

  /**
   * Return true and empty string if config is valid, return false and error message if config is invalid.
   */
  override def validate(): (Boolean, String) = {
    (true, "")
  }

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {
    var frame = df
    val attrs = config.getStepAttributes

    OperatorImplementMethod.get(config.getStepType) match {
      // execute java/spark jar
      case OperatorImplementMethod.PackageJava | OperatorImplementMethod.PackageSpark => {
        val userDefineOperator = new UserDefineOperator()
        val operator = ClassUtils.getUserOperatorImpl(userDefineOperator)
        if (attrs.containsKey("jarUrl")) {
          spark.sparkContext.addJar(attrs.get("jarUrl").toString)
          userDefineOperator.setJarPath(attrs.get("jarUrl").toString)
        }

        operator match {
          // process user define java jar(map function)
          case operator: ComputeOperator => frame = computeOperatorProcess(spark, df, userDefineOperator, operator, config)
          // process user define spark jar(single dataset)
          case dfProcess: SparkProcessProxy => frame = computeSparkProcess(spark, df, userDefineOperator, dfProcess)
          // other
          case _ => throw new RuntimeException(s"Unsupported define operator: [${userDefineOperator.getClassFullName}], please check it!")
        }
      }
      // execute java/scala code
      case OperatorImplementMethod.ScriptJava | OperatorImplementMethod.ScriptJava => {
        val codeBlock = attrs.get("codeBlock").toString
        val invokeInfo = new InvokeInfo("", attrs.get("methodName").toString, CommonUtil.portalField2ComputeField(config.getOutputFields))
        invokeInfo.setCode(codeBlock)
        frame = processDynamicCode(spark, frame, invokeInfo, config)
      }
      // execute sql
      case OperatorImplementMethod.ScriptSql => {
        val codeBlock = attrs.get("codeBlock").toString
        frame = spark.sql(codeBlock)
      }
      // execute python
      case OperatorImplementMethod.CommandPython => {
        frame = pipeLineProcess(spark, frame, "python", Array[String](), "")
      }
      // execute shell
      case OperatorImplementMethod.CommandShell => {
        frame = pipeLineProcess(spark, frame, "sh", Array[String](), "")
      }
      case _ => frame = df
    }

    frame
  }

}
