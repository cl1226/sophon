package com.scistor.operator.kafka.kerberos

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.alibaba.fastjson.JSONObject
import com.scistor.operator.ConstantsUtil.{randomAddress, randomName, randomUrl}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.config.SaslConfigs
import org.slf4j.LoggerFactory

object KafkaKbsProducer {

  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    //    val conf = new Configuration(true)
    //    conf.set("hadoop.security.authentication", "Kerberos")
    //    System.setProperty("keytab", "C:\\Users\\chenlou\\Desktop\\fsdownload\\cl.keytab")
    //    System.setProperty("java.security.krb5.conf", "C:\\Users\\chenlou\\Desktop\\fsdownload\\krb5.conf")
    //    UserGroupInformation.setConfiguration(conf)

    //    UserGroupInformation.loginUserFromKeytab("cl@HADOOP.COM", System.getProperty("keytab"))
    println("认证成功")

    System.setProperty("java.security.auth.login.config", "C:\\Users\\chenlou\\Desktop\\fsdownload\\sparkkafkajaas.conf")
    System.setProperty("java.security.krb5.conf", "C:\\Users\\chenlou\\Desktop\\fsdownload\\krb5.conf")
    //    System.setProperty("keytab", "C:\\Users\\chenlou\\Desktop\\fsdownload\\cl.keytab")

    val props = new Properties()
    props.put("bootstrap.servers", "scistor21:6667")
    props.put("batch.size", "16384")
    props.put("buffer.memory", "33554432")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT")
    props.put(SaslConfigs.SASL_MECHANISM, "GSSAPI")
    props.setProperty("sasl.kerberos.service.name", "kafka")

    val producer = new KafkaProducer[String, String](props)

    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val topic = "source01"

    logger.info("begin")

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
      //      logger.info(record.toString)

      producer.send(record)

      Thread.sleep(1000)

    }

    producer.close()
  }

}
