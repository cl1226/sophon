package com.scistor.operator

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.alibaba.fastjson.JSONObject
import com.scistor.operator.ConstantsUtil.{randomAddress, randomName, randomUrl}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object KafkaProducerDemo {

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
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    while (true) {

      val calendar = Calendar.getInstance()
      val timestamp = format.format(calendar.getTime)

      val json = new JSONObject()
      json.put("name", randomName())
      json.put("address", randomAddress())
      json.put("url", randomUrl())
      json.put("create_time", timestamp)

      val record = new ProducerRecord[String, String](topic, "", json.toJSONString)

      println(record)

      producer.send(record)

      Thread.sleep(duration.toLong)
    }

    producer.close()


  }

}
