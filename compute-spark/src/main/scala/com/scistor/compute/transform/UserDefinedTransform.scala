package com.scistor.compute.transform

import com.scistor.compute.SparkJobStarter.viewTableMap
import com.scistor.compute.apis.BaseTransform
import com.scistor.compute.interfacex.{ComputeOperator, SparkProcessProxy}
import com.scistor.compute.model.remote.{OperatorImplementMethod, StreamFieldDTO, TransStepDTO}
import com.scistor.compute.model.spark.{InvokeInfo, UserDefineOperator}
import com.scistor.compute.until.ClassUtils
import com.scistor.compute.utils.{CommonUtil, SparkInfoTransfer}
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
    frame.createOrReplaceTempView(config.getName)

    // execute java/spark jar
    val attrs = config.getStepAttributes
    spark.sparkContext.addJar(attrs.get("jarUrl").toString)
    val userDefineOperator = new UserDefineOperator()
    userDefineOperator.setJarPath(attrs.get("jarUrl").toString)
    val operator = ClassUtils.getUserOperatorImpl(userDefineOperator)

    operator match {
      // process user define java jar(map function)
      case operator: ComputeOperator => frame = computeOperatorProcess(spark, df, userDefineOperator, operator, config)
      // process user define spark jar(single dataset)
      case dfProcess: SparkProcessProxy => frame = computeSparkProcess(spark, df, userDefineOperator, dfProcess)
      // other
      case _ => throw new RuntimeException(s"Unsupported define operator: [${userDefineOperator.getClassFullName}], please check it!")
    }

    // execute java/scala code, sql, python/shell
    val codeBlock = attrs.get("codeBlock").toString
    codeBlock match {
      case "" =>
      case _ => {
        OperatorImplementMethod.get(config.getStepType) match {
          case OperatorImplementMethod.ScriptJava | OperatorImplementMethod.ScriptJava => {
            val invokeInfo = new InvokeInfo("", attrs.get("methodName").toString, CommonUtil.portalField2ComputeField(config.getOutputFields))
            invokeInfo.setCode(attrs.get("codeBlock").toString)
            frame = processDynamicCode(spark, frame, invokeInfo, config)
          }
          case OperatorImplementMethod.ScriptSql => frame = spark.sql(codeBlock)
          case OperatorImplementMethod.CommandPython => {
            frame = pipeLineProcess(spark, frame, "python", Array[String](), "")
          }
          case OperatorImplementMethod.CommandShell => {
            frame = pipeLineProcess(spark, frame, "sh", Array[String](), "")
          }
        }

      }
    }

    registerTempView(config.getName, frame)

    CommonUtil.writeSimpleData(frame, s"${SparkInfoTransfer.jobName} - ${config.getName}")

    df
  }

  private[scistor] def registerTempView(tableName: String, ds: Dataset[Row]): Unit = {
    ds.createOrReplaceTempView(tableName)
    viewTableMap += (tableName -> ds)
  }

}
