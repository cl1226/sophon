package com.scistor.compute.interfacex

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

import scala.collection.mutable.ArrayBuffer

trait KafkaStreamingProcess extends Serializable {
  def processKafkaData(record: ConsumerRecord[String, Array[Byte]]): java.util.Map[String,Object]
}

abstract class KafkaStreamingProcessProxy extends KafkaStreamingProcess{
  def createTableFromStream(
    spark: SparkSession,
    stream: InputDStream[ConsumerRecord[String, Array[Byte]]],
    newTableSchme: StructType
    ): DStream[Row] = {
      val resultStream = stream.mapPartitions(partion => {
        val newPartion = partion.map(record => {
          record2Row(processKafkaData(record), newTableSchme)
        })
        newPartion
      }).filter(_.isDefined).map(_.get)
    resultStream
  }

  private def record2Row(
    dataMap: java.util.Map[String,Object],
    newTableSchme: StructType
    ): Option[Row] = {
      if(dataMap == null || dataMap.size() == 0){
        Option.empty[Row]
      }else {
        val rowSchme = newTableSchme.toList.map(s=>s.name)
        val buffer = ArrayBuffer[Any]()
        rowSchme.foreach(field => {
          buffer += dataMap.getOrDefault(field,"")
        })
        //      new GenericRow(buffer.toArray)
        Option.apply(new GenericRowWithSchema(buffer.toArray,newTableSchme))
      }
  }
}
