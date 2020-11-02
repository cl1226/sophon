package com.scistor.compute.input.sparkstreaming.kafkaStreamProcess

import com.scistor.compute.input.sparkstreaming.KafkaStream
import com.scistor.compute.model.remote.TransStepDTO
import com.scistor.compute.model.spark.SourceAttribute
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSchemaUtil.parseStructType
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class CsvStreamProcess(config: TransStepDTO) extends KafkaStream {

  override def rdd2dataset(spark: SparkSession, rdd: RDD[ConsumerRecord[String, String]]): Dataset[Row] = {

    val schema = parseStructType(config.getId)

    val transformedRDD = rdd.map(record => {
      record.value()
    })

    import spark.implicits._

    val df = spark.read
      .schema(schema)
      .csv(spark.createDataset(transformedRDD))

    df

  }

}
