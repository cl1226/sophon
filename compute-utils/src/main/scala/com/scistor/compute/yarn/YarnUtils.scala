package com.scistor.compute.yarn

import com.scistor.compute.common.RemoteConfig
import com.scistor.compute.common.ShellUtils._

import java.util.Properties
import java.util.regex.{Matcher, Pattern}
import java.util
import java.util.concurrent.{ArrayBlockingQueue, Callable, FutureTask, ThreadPoolExecutor, TimeUnit}
import scala.collection.JavaConverters.asScalaBufferConverter

object YarnUtils {

  private var prop = new Properties
  private var remoteConfig = new RemoteConfig
  private val executor = new ThreadPoolExecutor(5, 20, 10, TimeUnit.SECONDS, new ArrayBlockingQueue[Runnable](20))

  def init(properties: Properties, remoteConfig: RemoteConfig) = {
    this.remoteConfig = remoteConfig
    this.prop = properties
    this
  }

  def getApplicationIdByJobName(jobName: String): String = {
    var application_id = ""
    val cmd = prop.getProperty("get_job_id").replaceAll("jobName", jobName)
    val result = runShellBlock(remoteConfig, cmd)

    val p = Pattern.compile(prop.getProperty("get_job_id_reg"))
    val matcher: Matcher = p.matcher(result)
    if (matcher.find) application_id = matcher.group(prop.getProperty("get_job_id_reg_group").toInt)
    application_id
  }

  def getApplicationStatusByArray(jobNames: util.ArrayList[String]): util.ArrayList[FutureTask[util.HashMap[String, String]]] = {
    val futureList = new util.ArrayList[FutureTask[util.HashMap[String, String]]]
    jobNames.asScala.map(job => {
      val future = new FutureTask[util.HashMap[String, String]](new Callable[util.HashMap[String, String]] {
        override def call(): util.HashMap[String, String] = {
          val res: util.HashMap[String, String] = getApplicationStatusByJobName(job)
          res.put("yarnid", job)
          res
        }
      })
      executor.submit(future)
      futureList.add(future)
    })
    futureList
  }

  def getApplicationStatusByArray2(jobNames: util.ArrayList[String]): util.ArrayList[util.HashMap[String, String]] = {
    val resultList = new util.ArrayList[util.HashMap[String, String]]
    jobNames.asScala.map(job => {
      val res: util.HashMap[String, String] = getApplicationStatusByJobName(job)
      res.put("yarnid", job)
      resultList.add(res)
    })
    resultList
  }

  def getApplicationStatusByJobName(jobName: String): util.HashMap[String, String] = {
    var jobStatus = ""
    var finalJobStatus = ""
    val application_id = getApplicationIdByJobName(jobName)

    val cmd = prop.getProperty("get_job_status").replaceAll("application_id", application_id)
    val res = runShellBlock(remoteConfig, cmd)
    val p = Pattern.compile(prop.getProperty("get_job_status_reg"))
    val matcher: Matcher = p.matcher(res)
    if (matcher.find) jobStatus = matcher.group(prop.getProperty("get_job_status_reg_group").toInt)

    val p2 = Pattern.compile(prop.getProperty("get_job_final_status_reg"))
    val matcher2: Matcher = p2.matcher(res)
    if (matcher2.find) finalJobStatus = matcher2.group(prop.getProperty("get_job_status_reg_group").toInt)
    val map = new util.HashMap[String, String]()
    map.put("Status", jobStatus)
    map.put("FinalStatus", finalJobStatus)
    map.put("id", application_id)
    map
  }

  def getApplicationLogsByJobName(jobName: String): String = {
    val application_id = getApplicationIdByJobName(jobName)
    val cmd = prop.getProperty("get_job_error_log").replaceAll("application_id", application_id)
    val jobLog = runShellBlock(remoteConfig, cmd)
    jobLog
  }

  def killApplicationByJobName(jobName: String): Unit = {
    val application_id = getApplicationIdByJobName(jobName)
    val cmd = prop.getProperty("kill_job_command").replaceAll("application_id", application_id)
    runShell(remoteConfig, cmd)
  }

  def main(args: Array[String]): Unit = {
    val remoteConfig = new RemoteConfig("192.168.31.77", 22, "root", "123456")

    val properties = new Properties
    properties.load(this.getClass.getResourceAsStream("/yarn_cmd.properties"))
//    properties.asScala.foreach(x => {
//      println(s"${x._1}: ${x._2}")
//    })

    val utils = YarnUtils.init(properties, remoteConfig)

    val array: util.ArrayList[String] = new util.ArrayList[String](5)
    array.add("循环3_6685ef06474f493e9e6001c966475b27")
    array.add("循环1_0ac3284b41ee4199bbadcff6ece7ae35")
    array.add("循环2_3e711af8a81248be8f826d91fc9cdae0")
    array.add("循环5_58bf5158493a454db8eddfeb48576b68")
    array.add("循环4_af2c7be2758e472d9bcb92ceead8c48c")

    val resultList = utils.getApplicationStatusByArray2(array)
    resultList.asScala.foreach(println)
  }
}
