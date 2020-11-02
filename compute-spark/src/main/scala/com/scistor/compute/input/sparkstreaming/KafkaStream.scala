package com.scistor.compute.input.sparkstreaming

import java.util.Properties

import com.scistor.compute.apis.BaseStreamingInput
import com.scistor.compute.input.sparkstreaming.kafkaStreamProcess.{CsvStreamProcess, JsonStreamProcess}
import com.scistor.compute.model.remote.TransStepDTO
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

  var config: TransStepDTO = _

  var kafkaParams: Map[String, String] = _

  /**
   * Set Config.
   **/
  override def setConfig(config: TransStepDTO): Unit = {
    this.config = config
  }

  /**
   * Get Config.
   **/
  override def getConfig(): TransStepDTO = config

  /**
   * Return true and empty string if config is valid, return false and error message if config is invalid.
   */
  override def validate(): (Boolean, String) = {
    (true, "")
  }

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val attrs = config.getStepAttributes

    val props = new Properties()
    props.setProperty("format", "json")
    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("bootstrap.servers", attrs.getOrDefault("bootstrap_urls", "").toString)
    props.setProperty("group.id", attrs.getOrDefault("group_id", "").toString)
    props.setProperty("enable.auto.commit", "false")

    if (attrs.containsKey("kerberos") && attrs.getOrElse("kerberos", "").toString.equals("true")){
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
    val attrs = config.getStepAttributes
    val topics = attrs.get("topic").toString.split(",").toSet
    val inputDStream : InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe(topics, kafkaParams))

    inputDStream
  }

  override def start(spark: SparkSession, ssc: StreamingContext, handler: Dataset[Row] => Unit): Unit = {
    val attrs = config.getStepAttributes
    val inputDStream = getDStream(ssc)

    val handlerInstance: KafkaStream = DecodeType.valueOf(attrs.get("decodeType").toString) match {
      case DecodeType.JSON => new JsonStreamProcess(config)
      case DecodeType.CSV => new CsvStreamProcess(config)
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
        logInfo(s"${config.getStepAttributes.get("groupid")} consumer 0 record")
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
