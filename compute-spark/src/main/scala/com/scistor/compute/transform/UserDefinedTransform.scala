package com.scistor.compute.transform

import java.util

import com.scistor.compute.apis.BaseTransform
import com.scistor.compute.interfacex.{ComputeOperator, SparkProcessProxy}
import com.scistor.compute.model.remote.{OperatorImplementMethod, TransStepDTO}
import com.scistor.compute.model.spark.{InvokeInfo, UserDefineOperator}
import com.scistor.compute.until.ClassUtils
import com.scistor.compute.utils.CommonUtil
import org.apache.spark.sql.ComputeProcess.{executeJavaProcess, executeSparkProcess, pipeLineProcess, processDynamicCode}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

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
    val attrs = config.getStepAttributes
    OperatorImplementMethod.get(config.getStepType) match {
      case OperatorImplementMethod.PackageJava | OperatorImplementMethod.PackageSpark => {
        if (!attrs.containsKey("packageName") || !attrs.containsKey("methodName")) {
          (false, s"please special ${config.getStepType} packageName and methodName as string")
        } else {
          (true, "")
        }
      }
      case OperatorImplementMethod.ScriptJava | OperatorImplementMethod.ScriptScala => {
        if (!attrs.containsKey("codeBlock") || !attrs.containsKey("methodName")) {
          (false, s"please special ${config.getStepType} codeBlock and methodName as string")
        } else {
          (true, "")
        }
      }
      case OperatorImplementMethod.ScriptSql => {
        if (!attrs.containsKey("codeBlock")) {
          (false, s"please special ${config.getStepType} codeBlock as string")
        } else {
          (true, "")
        }
      }
      // execute python/shell
      case OperatorImplementMethod.CommandPython | OperatorImplementMethod.CommandShell => {
        if (!attrs.containsKey("codeFileUrl")) {
          (false, s"please special ${config.getStepType} codeFileUrl as string")
        } else {
          (true, "")
        }
      }
    }
  }

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {
    var frame = df
    val attrs = config.getStepAttributes

    println(s"[INFO] 自定义算子 [${config.getStepType}] properties: ")
    attrs.foreach(entry => {
      val (key, value) = entry
      println("\t" + key + " = " + value)
    })

    OperatorImplementMethod.get(config.getStepType) match {
      // execute java/spark jar
      case OperatorImplementMethod.PackageJava | OperatorImplementMethod.PackageSpark => {
        val userDefineOperator = new UserDefineOperator()
        userDefineOperator.setClassFullName(attrs.get("packageName").toString)
        userDefineOperator.setMethodName(attrs.get("methodName").toString)
        val operator = ClassUtils.getUserOperatorImpl(userDefineOperator)
        if (attrs.containsKey("jarUrl")) {
          spark.sparkContext.addJar(attrs.get("jarUrl").toString)
          userDefineOperator.setJarPath(attrs.get("jarUrl").toString)
        }

        operator match {
          // process user define java jar(map function)
          case operator: ComputeOperator => frame = executeJavaProcess(spark, df, operator, config)
          // process user define spark jar(single dataset)
          case dfProcess: SparkProcessProxy => frame = executeSparkProcess(spark, df, dfProcess, config)
          // other
          case _ => throw new RuntimeException(s"Unsupported define operator: [${userDefineOperator.getClassFullName}], please check it!")
        }
      }
      // execute java/scala code
      case OperatorImplementMethod.ScriptJava | OperatorImplementMethod.ScriptScala => {
        val codeBlock = attrs.get("codeBlock").toString
        val invokeInfo = new InvokeInfo("", attrs.get("methodName").toString, CommonUtil.portalField2ComputeField(config.getInputFields))
        invokeInfo.setCode(codeBlock)
        frame = processDynamicCode(spark, frame, invokeInfo, config)
      }
      // execute sql
      case OperatorImplementMethod.ScriptSql => {
        val codeBlock = attrs.get("codeBlock").toString
        frame = spark.sql(codeBlock)
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

}
