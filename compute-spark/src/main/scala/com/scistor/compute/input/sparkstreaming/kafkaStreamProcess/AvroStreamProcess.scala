package com.scistor.compute.input.sparkstreaming.kafkaStreamProcess

import com.scistor.compute.input.sparkstreaming.KafkaStream
import com.scistor.compute.model.remote.TransStepDTO
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{ComputeDataType, DataTypes, StructField}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._
import scala.collection.mutable

class AvroStreamProcess(config: TransStepDTO) extends KafkaStream {
  override def rdd2dataset(spark: SparkSession, rdd: RDD[ConsumerRecord[String, String]]): Dataset[Row] = {
    val structFields = mutable.ArrayBuffer[StructField]()
    config.getOutputFields.foreach(output => {
      structFields += DataTypes.createStructField(output.getStreamFieldName, ComputeDataType.fromStructField(output.getFieldType), true)
    })
    val schema = DataTypes.createStructType(structFields.toArray)

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
