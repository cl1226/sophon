package com.scistor.compute.input.sparkstreaming

import java.util.Properties

import com.scistor.compute.apis.BaseStreamingInput
import com.scistor.compute.input.sparkstreaming.kafkaStreamProcess.{CsvStreamProcess, JsonStreamProcess}
import com.scistor.compute.model.spark.{DecodeType, SinkAttribute, SourceAttribute}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSchemaUtil.parseStructType
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, RowFactory, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}

import scala.collection.JavaConversions._

class KafkaStream extends BaseStreamingInput[ConsumerRecord[String, String]]{

  var sourceAttribute: SourceAttribute = _

  var kafkaParams: Map[String, String] = _

  /**
   * Set SourceAttribute.
   **/
  override def setSource(source: SourceAttribute): Unit = {
    this.sourceAttribute = source
  }

  /**
   * get SourceAttribute.
   **/
  override def getSource(): SourceAttribute = sourceAttribute

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val props = new Properties()
    props.setProperty("format", "json")
    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("bootstrap.servers", sourceAttribute.bootstrap_urls)
    props.setProperty("group.id", sourceAttribute.groupid)
    props.setProperty("enable.auto.commit", "false")

    if (sourceAttribute.isKerberos){
      props.setProperty("security.protocol", "SASL_PLAINTEXT")
      props.setProperty("sasl.kerberos.service.name", "kafka")
    }

    println("[INFO] Kafka Input properties: ")
    props.foreach(entry => {
      val (key, value) = entry
      println("\t" + key + " = " + value)
    })

    kafkaParams = props.foldRight(Map[String, String]())((entry, map) => {
      map + (entry._1 -> entry._2)
    })

  }

  override def getDStream(ssc: StreamingContext): DStream[ConsumerRecord[String, String]] = {

    val topics = sourceAttribute.topic.split(",").toSet
    val inputDStream : InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe(topics, kafkaParams))

    inputDStream
  }

  override def start(spark: SparkSession, ssc: StreamingContext, handler: Dataset[Row] => Unit): Unit = {

    val inputDStream = getDStream(ssc)

    val handlerInstance: KafkaStream = sourceAttribute.decodeType match {
      case DecodeType.JSON => new JsonStreamProcess(sourceAttribute)
      case DecodeType.CSV => new CsvStreamProcess(sourceAttribute)
      case _ => null
    }

    inputDStream.foreachRDD(rdd => {
      if (rdd.count() > 0) {
        // do not define offsetRanges in KafkaStream Object level, to avoid commit wrong offsets
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        val dataset = handlerInstance.rdd2dataset(spark, rdd)

        handler(dataset)

        // update offset after output
        inputDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
        for (offsets <- offsetRanges) {
          val fromOffset = offsets.fromOffset
          val untilOffset = offsets.untilOffset
          if (untilOffset != fromOffset) {
            logInfo(s"completed consuming topic: ${offsets.topic} partition: ${offsets.partition} from ${fromOffset} until ${untilOffset}")
          }
        }
      } else {
        logInfo(s"${sourceAttribute.groupid} consumer 0 record")
      }
    })
  }

  override def rdd2dataset(spark: SparkSession, rdd: RDD[ConsumerRecord[String, String]]): Dataset[Row] = {

    val transformedRDD = rdd.map(record => {
      (record.topic(), record.value())
    })

    val rowsRDD = transformedRDD.map(element => {
      element match {
        case (topic, message) => {
          RowFactory.create(topic, message)
        }
      }
    })

    val schema = StructType(
      Array(StructField("topic", DataTypes.StringType), StructField("raw_message", DataTypes.StringType)))
    spark.createDataFrame(rowsRDD, schema)
  }

}
