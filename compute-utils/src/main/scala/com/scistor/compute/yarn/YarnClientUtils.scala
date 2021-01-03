package com.scistor.compute.yarn

import com.scistor.compute.common.ShellUtils._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration

import java.net.{URI, URL}
import java.util.Properties
import java.util.regex.{Matcher, Pattern}
import java.util
import scala.collection.JavaConverters._

class YarnClientUtils {

  private var prop = new Properties

  def this(properties: Properties) {
    this()
    this.prop = properties
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
        })
      }
      case _ => {
        // yarn client api
        val yarnSitePath = prop.getProperty("yarn_site_path")
        val conf = new YarnConfiguration()
        conf.addResource(new Path(URI.create(yarnSitePath)))
        val yarnClient = YarnClient.createYarnClient()
        yarnClient.init(conf)
        yarnClient.start()
        val applications = yarnClient.getApplications(util.EnumSet.of(
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
            map.put("yarnId", app(0).getName)
            resultMap.put(app(0).getName, map)
          } else {
            map.put("Status", "")
            map.put("FinalStatus", "")
            map.put("id", "")
            map.put("yarnId", "")
            resultMap.put(job, map)
          }
        })
      }
    }
    resultMap
  }

  private def getApplicationIdByJobName(jobName: String): String = {
    var application_id = ""
    val cmd = prop.getProperty("get_job_id").replaceAll("jobName", jobName)
    val result = runShellBlock(cmd, prop.getProperty("prefix", ""))

    val p = Pattern.compile(prop.getProperty("get_job_id_reg"))
    val matcher: Matcher = p.matcher(result)
    if (matcher.find) application_id = matcher.group(prop.getProperty("get_job_id_reg_group").toInt)
    application_id
  }

  private def getApplicationStatusByJobName(jobName: String): util.HashMap[String, String] = {
    var jobStatus = ""
    var finalJobStatus = ""
    val application_id = getApplicationIdByJobName(jobName)

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
    val method = prop.getProperty("method", "shell")
    method match {
      case "shell" => {
        val application_id = getApplicationIdByJobName(jobName)
        val cmd = prop.getProperty("get_job_error_log").replaceAll("application_id", application_id)
        jobLog = runShellBlock(cmd, prop.getProperty("prefix", ""))
      }
      case _ => {
        // http/https restful
      }
    }
    jobLog
  }

  def killApplicationByJobName(jobName: String): Unit = {
    val application_id = getApplicationIdByJobName(jobName)
    if (!"".equals(application_id)) {
      val cmd = prop.getProperty("kill_job_command").replaceAll("application_id", application_id)
      runShell(cmd, prop.getProperty("prefix", ""))
    }
  }

}

object YarnClientUtils {
  def main(args: Array[String]): Unit = {
    val properties = new Properties
    properties.load(this.getClass.getResourceAsStream("/yarn_cmd.properties"))
    val yarnUtils = new YarnClientUtils(properties)

    val array: util.ArrayList[String] = new util.ArrayList[String](5)
    array.add("内置算子模型2_93b085cd64834153bd1318c78351261b")

    val resultMap = yarnUtils.getApplicationStatusByArray(array)
    resultMap.asScala.foreach(println)
  }
}
