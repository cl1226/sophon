package com.scistor.compute.yarn

import com.scistor.compute.common.ShellUtils
import com.scistor.compute.common.ShellUtils._
import com.sun.security.auth.callback.TextCallbackHandler
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration

import java.io.File
import java.util.Properties
import java.util.regex.{Matcher, Pattern}
import java.util
import javax.security.auth.login.LoginContext
import scala.collection.JavaConverters._

class YarnClientUtils {

  private var prop = new Properties
  private val yarnClient = YarnClient.createYarnClient()
  private var debug = false

  def this(properties: Properties) {
    this()
    this.prop = properties
    if (this.prop.containsKey("debug")) {
      this.debug = this.prop.getProperty("debug", "false").equals("true")
    }
    ShellUtils.debug = this.debug

    var yarnSitePath: String = ""
    if (prop.containsKey("yarn_site_path")) {
      yarnSitePath = prop.getProperty("yarn_site_path", "")
    }
    var method: String = "shell"
    if (prop.containsKey("method")) {
      method = prop.getProperty("method", "shell")
    }
    if (method.equals("api") && !"".equals(yarnSitePath)) {
      val conf = new YarnConfiguration()
      conf.addResource(new File(yarnSitePath).toURI.toURL)
      yarnClient.init(conf)
      yarnClient.start()
    }
  }

  def getApplicationStatusByArray(jobNames: util.ArrayList[String]): util.HashMap[String, util.HashMap[String, String]] = {
    val resultMap = new util.HashMap[String, util.HashMap[String, String]]
    val method = prop.getProperty("method", "shell")
    method match {
      case "shell" => {
        jobNames.asScala.map(job => {
          val res: util.HashMap[String, String] = getApplicationStatusByJobName(job)
          res.put("yarnid", job)
          resultMap.put(job, res)
          if (debug) {
            println(resultMap.toString)
          }
        })
      }
      case _ => {
        // yarn client api
        val applicationTypes = new util.HashSet[String]()
        applicationTypes.add("SPARK")
        try {
          val applications = yarnClient.getApplications(applicationTypes, util.EnumSet.of(
            YarnApplicationState.ACCEPTED,
            YarnApplicationState.RUNNING,
            YarnApplicationState.FINISHED,
            YarnApplicationState.FAILED,
            YarnApplicationState.KILLED))
          val apps = applications.asScala.filter(app => {
            jobNames.contains(app.getName)
          })
          jobNames.asScala.foreach(job => {
            val app = apps.filter(_.getName.equals(job)).toArray
            val map = new util.HashMap[String, String]()
            if (app.length > 0) {
              map.put("Status", app(0).getYarnApplicationState.name())
              map.put("FinalStatus", app(0).getFinalApplicationStatus.name())
              map.put("id", app(0).getApplicationId.toString)
              map.put("yarnid", job)
              resultMap.put(job, map)
            } else {
              map.put("Status", "")
              map.put("FinalStatus", "")
              map.put("id", "")
              map.put("yarnid", "")
              resultMap.put(job, map)
            }
          })
          if (this.debug) {
            println(resultMap.toString)
          }
        } catch {
          case e: Exception => e.printStackTrace()
        }
      }
    }
    resultMap
  }

  private def getApplicationIdByJobName(jobName: String): String = {
    val method = prop.getProperty("method", "shell")
    method match {
      case "shell" => {
        var application_id = ""
        val cmd = prop.getProperty("get_job_id").replaceAll("jobName", jobName)
        val prefix = prop.getProperty("prefix", "")
        val result = runShellBlock(cmd, prefix)

        val p = Pattern.compile(prop.getProperty("get_job_id_reg"))
        val matcher: Matcher = p.matcher(result)
        if (matcher.find) application_id = matcher.group(prop.getProperty("get_job_id_reg_group").toInt)
        application_id
      }
      case _ => {
        val applicationTypes = new util.HashSet[String]()
        applicationTypes.add("SPARK")
        val applications = yarnClient.getApplications(applicationTypes, util.EnumSet.of(
          YarnApplicationState.ACCEPTED,
          YarnApplicationState.RUNNING,
          YarnApplicationState.FINISHED,
          YarnApplicationState.FAILED,
          YarnApplicationState.KILLED))
        val app = applications.asScala.filter(_.getName.equals(jobName))
        if (!app.isEmpty) {
          app(0).getApplicationId.toString
        } else {
          ""
        }
      }
    }

  }

  private def getApplicationStatusByJobName(jobName: String): util.HashMap[String, String] = {
    var jobStatus = ""
    var finalJobStatus = ""
    val application_id = getApplicationIdByJobName(jobName)

    if (debug) {
      println(s"jobName: $jobName -- application_id: $application_id")
    }

    val map = new util.HashMap[String, String]()

    !"".equals(application_id) match {
      case true => {
        val cmd = prop.getProperty("get_job_status").replaceAll("application_id", application_id)
        val res = runShellBlock(cmd, prop.getProperty("prefix", ""))
        val p = Pattern.compile(prop.getProperty("get_job_status_reg"))
        val matcher: Matcher = p.matcher(res)
        if (matcher.find) jobStatus = matcher.group(prop.getProperty("get_job_status_reg_group").toInt)

        val p2 = Pattern.compile(prop.getProperty("get_job_final_status_reg"))
        val matcher2: Matcher = p2.matcher(res)
        if (matcher2.find) finalJobStatus = matcher2.group(prop.getProperty("get_job_status_reg_group").toInt)
        map.put("Status", jobStatus)
        map.put("FinalStatus", finalJobStatus)
        map.put("id", application_id)
        map
      }
      case false => map
    }
  }

  def getApplicationLogsByJobName(jobName: String): String = {
    var jobLog = ""
    val application_id = getApplicationIdByJobName(jobName)
    if (debug) {
      println(s"jobName: $jobName -- application_id: $application_id")
    }
    val cmd = prop.getProperty("get_job_error_log").replaceAll("application_id", application_id)
    jobLog = runShellBlock(cmd, prop.getProperty("prefix", ""))
    jobLog
  }

  private def getApplicationIdByJobNameAndApi(jobName: String) = {
    val applicationTypes = new util.HashSet[String]()
    applicationTypes.add("SPARK")
    val applications = yarnClient.getApplications(applicationTypes, util.EnumSet.of(
      YarnApplicationState.ACCEPTED,
      YarnApplicationState.RUNNING,
      YarnApplicationState.FINISHED,
      YarnApplicationState.FAILED,
      YarnApplicationState.KILLED))
    val app = applications.asScala.filter(_.getName.equals(jobName))
    if (!app.isEmpty) {
      app(0).getApplicationId
    } else {
      null
    }
  }

  def killApplicationByJobName(jobName: String): Unit = {
    val method = prop.getProperty("method", "shell")
    method match {
      case "shell" => {
        val application_id = getApplicationIdByJobName(jobName)
        if (!"".equals(application_id)) {
          val cmd = prop.getProperty("kill_job_command").replaceAll("application_id", application_id)
          runShell(cmd, prop.getProperty("prefix", ""))
        }
      }
      case _ => {
        val applicationId = getApplicationIdByJobNameAndApi(jobName)
        if (applicationId != null) {
          yarnClient.killApplication(applicationId)
        }
      }
    }
  }
}

object YarnClientUtils {
  def main(args: Array[String]): Unit = {
    val properties = new Properties
    properties.load(this.getClass.getResourceAsStream("/yarn_cmd.properties"))
    val yarnUtils = new YarnClientUtils(properties)

    val array: util.ArrayList[String] = new util.ArrayList[String](5)
    array.add("hive2hive_358a020500fa4cac8c541e05304ae181")
//
    val resultMap = yarnUtils.getApplicationStatusByArray(array)

//    yarnUtils.killApplicationByJobName("kafka2kafka模型_74f595414aab47dd82d293a30bf4f1b2")
    resultMap.asScala.foreach(println)
  }
}
