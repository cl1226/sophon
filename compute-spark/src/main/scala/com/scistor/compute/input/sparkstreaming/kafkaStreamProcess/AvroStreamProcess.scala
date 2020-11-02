package com.scistor.compute.input.sparkstreaming.kafkaStreamProcess

import com.scistor.compute.input.sparkstreaming.KafkaStream
import com.scistor.compute.model.remote.TransStepDTO
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class AvroStreamProcess(config: TransStepDTO) extends KafkaStream {
  override def rdd2dataset(spark: SparkSession, rdd: RDD[ConsumerRecord[String, String]]): Dataset[Row] = {
    super.rdd2dataset(spark, rdd)
  }
}
