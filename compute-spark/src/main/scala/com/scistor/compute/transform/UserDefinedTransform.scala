package com.scistor.compute.transform

import java.util

import com.scistor.compute.apis.BaseTransform
import com.scistor.compute.interfacex.{ComputeOperator, SparkProcessProxy}
import com.scistor.compute.model.remote.{OperatorImplementMethod, TransStepDTO}
import com.scistor.compute.model.spark.{InvokeInfo, UserDefineOperator}
import com.scistor.compute.until.ClassUtils
import com.scistor.compute.utils.CommonUtil
import org.apache.spark.sql.ComputeProcess.{computeOperatorProcess, computeSparkProcess, pipeLineProcess, processDynamicCode}
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
    (true, "")
  }

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {
    var frame = df
    val attrs = config.getStepAttributes

    println(s"[INFO] 自定义算子 <${config.getStepType}> input properties: ")
    attrs.foreach(entry => {
      val (key, value) = entry
      println("\t" + key + " = " + value)
    })

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
