package com.scistor.operator.kafka.kerberos

import java.util.Properties

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.config.SaslConfigs

object KafkaKbsConsumer {

  def main(args: Array[String]): Unit = {
    System.setProperty("java.security.auth.login.config", "C:\\Users\\chenlou\\Desktop\\fsdownload\\sparkkafkajaas.conf")
    System.setProperty("java.security.krb5.conf", "C:\\Users\\chenlou\\Desktop\\fsdownload\\krb5.conf")

    val props = new Properties()
    props.put("bootstrap.servers", "scistor21:6667")
    props.put("batch.size", "16384")
    props.put("buffer.memory", "33554432")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("group.id", "logGroup")

    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT")
    props.put(SaslConfigs.SASL_MECHANISM, "GSSAPI")
    props.setProperty("sasl.kerberos.service.name", "kafka")

    val consumer = new KafkaConsumer[String, String](props)
    val topics = java.util.Arrays.asList("source01")
    consumer.subscribe(topics)
    var consumerRecords: ConsumerRecords[String, String] = null

    while (true) {
      consumerRecords = consumer.poll(1000)
      //遍历每一条记录
      import scala.collection.JavaConversions._
      for (consumerRecord <- consumerRecords) {
        val offset = consumerRecord.offset
        val partition = consumerRecord.partition
        val key = consumerRecord.key
        val value = consumerRecord.value
        System.out.println(offset + " " + partition + " " + key + " " + value)
      }
    }
  }

}
