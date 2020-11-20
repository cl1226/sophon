package com.scistor.operator

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.alibaba.fastjson.JSONObject
import com.scistor.operator.ConstantsUtil.{randomAddress, randomName, randomUrl}
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.GenericAvroCodecs
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object KafkaAVROProducerDemo {

  val USER_SCHEMA: String =
    s"""
       | {
       |    "fields": [
       |        { "name": "name", "type": "string" },
       |        { "name": "address", "type": "string" },
       |        { "name": "create_time", "type": "string" },
       |        { "name": "url", "type": "string" }
       |    ],
       |    "name": "Test",
       |    "type": "record"
       | }
       |""".stripMargin

  def main(args: Array[String]): Unit = {

    val brokers = args(0)
    val topic = args(1)
    // 暂停毫秒
    val duration = args(2)

    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("acks", "all")
    props.put("retries", "0")
    props.put("batch.size", "16384")
    props.put("linger.ms", "1")
    props.put("buffer.memory", "33554432")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")

    val parser = new Schema.Parser

    val schema = parser.parse(USER_SCHEMA)

    val recordInjection: Injection[GenericRecord, Array[Byte]] = GenericAvroCodecs.toBinary(schema)

    val producer = new KafkaProducer[String, Array[Byte]](props)

    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    while (true) {

      val calendar = Calendar.getInstance()
      val timestamp = format.format(calendar.getTime)

      val avroRecord: GenericData.Record = new GenericData.Record(schema)
      avroRecord.put("name", randomName())
      avroRecord.put("address", randomAddress())
      avroRecord.put("url", randomUrl())
      avroRecord.put("create_time", timestamp)

      val bytes = recordInjection.apply(avroRecord)

      val record = new ProducerRecord[String, Array[Byte]](topic, "", bytes)

      println(record)

      producer.send(record)

      Thread.sleep(duration.toLong)
    }

    producer.close()


  }

}
