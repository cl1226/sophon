package com.scistor.compute.utils

import com.scistor.compute.model.remote.{DataSourceStepType, DataSourceType, SparkTransDTO, StepSourceType, TransStepDTO}
import com.scistor.compute.redis.{JedisClient, JedisUtil}
import org.apache.spark.internal.Logging

import scala.collection.mutable
import scala.collection.JavaConverters._

object SparkInfoTransfer extends Logging {

  // job name
  var jobName: String = _
  // offline datasource map
  var staticInputs = collection.immutable.ListMap[String, TransStepDTO]()
  // online datasource map
  var streamingInputs = collection.immutable.ListMap[String, TransStepDTO]()
  // transform operators map
  var transforms = collection.immutable.ListMap[String, TransStepDTO]()
  // output datasource map
  var outputs = collection.immutable.ListMap[String, TransStepDTO]()
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
                  streamingInputs = streamingInputs + (step.getStepInfo.getName -> step.getStepInfo)
                }
                case _ => {
                  step.getStepInfo.setStrategy(info.getStrategy)
                  staticInputs = staticInputs + (step.getStepInfo.getName -> step.getStepInfo)
                }
              }
            }
            case DataSourceStepType.DataSourceOutput => {
              step.getStepInfo.setStepFrom(step.getStepFrom.get(0))
              outputs = outputs + (step.getStepInfo.getName -> step.getStepInfo)
            }
          }

        }
        case StepSourceType.Custom | StepSourceType.System => {
          // assembly user define or system operator step info
          step.getStepInfo.setStepFrom(step.getStepFrom.get(0))
          transforms = transforms + (step.getStepInfo.getName -> step.getStepInfo)
        }
        case _ => logError(s"unsupported [${step.getStepInfo.getStepSource}] type source")
      }

    })
  }

}
