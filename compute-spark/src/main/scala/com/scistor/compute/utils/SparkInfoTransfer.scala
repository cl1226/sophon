package com.scistor.compute.utils

import com.scistor.compute.model.remote.{DataSourceStepType, DataSourceType, OperatorImplementMethod, SparkTransDTO, StepAttributeByDataSourceDTO, StepSourceType, TransStepDTO}
import com.scistor.compute.redis.{JedisClient, JedisUtil}
import org.apache.spark.internal.Logging

import scala.collection.mutable
import scala.collection.JavaConverters._

object SparkInfoTransfer extends Logging {

  // job name
  var jobName: String = _
  // offline datasource map
  val staticInputs = mutable.Map[String, TransStepDTO]()
  // online datasource map
  val streamingInputs = mutable.Map[String, TransStepDTO]()
  // transform operators map
  val transforms = mutable.Map[String, TransStepDTO]()
  // output datasource map
  val outputs = mutable.Map[String, TransStepDTO]()
  // mysql config
  var jobInfo: SparkTransDTO = _
  // redis config
  var jedis: JedisClient = _

  def transfer(info: SparkTransDTO): Unit = {
    this.jobInfo = info
    this.jobName = info.getTransName
    this.jedis = JedisUtil.getJedisClient(info.getRedisConfig)
    info.getStepList.asScala.foreach(step => {
      StepSourceType.get(step.getStepInfo.getStepSource) match {
        case StepSourceType.DataSource => {
          // assembly datasource step info
          DataSourceStepType.get(step.getStepInfo.getDataSourceStepType) match {
            case DataSourceStepType.DataSourceInput => {
              DataSourceType.get(step.getStepInfo.getStepType) match {
                case DataSourceType.Kafka => {
                  streamingInputs += (step.getStepInfo.getName -> step.getStepInfo)
                }
                case _ => {
                  staticInputs += (step.getStepInfo.getName -> step.getStepInfo)
                }
              }
            }
            case DataSourceStepType.DataSourceOutput => {
              step.getStepInfo.setStepFrom(step.getStepFrom.get(0))
              outputs += (step.getStepInfo.getName -> step.getStepInfo)
            }
          }

        }
        case StepSourceType.Custom | StepSourceType.System => {
          // assembly user define or system operator step info
          step.getStepInfo.setStepFrom(step.getStepFrom.get(0))
          transforms += (step.getStepInfo.getName -> step.getStepInfo)
        }
        case _ => logError(s"unsupported [${step.getStepInfo.getStepSource}] type source")
      }

    })
  }

}
