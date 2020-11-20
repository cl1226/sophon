package com.scistor.compute.input.sparkstreaming.kafkaStreamProcess

import com.scistor.compute.input.sparkstreaming.KafkaStream
import com.scistor.compute.model.remote.TransStepDTO
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.GenericAvroCodecs
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{ComputeDataType, DataTypes, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._
import scala.collection.mutable

class AvroStreamProcess(config: TransStepDTO) extends KafkaStream {

  override def rdd2dataset(spark: SparkSession, rdd: RDD[ConsumerRecord[String, AnyRef]]): Dataset[Row] = {

    val attrs = config.getStepAttributes
    val avroSchema = attrs.get("avroSchema").toString

    val structFields = mutable.ArrayBuffer[StructField]()
    config.getOutputFields.foreach(output => {
      structFields += DataTypes.createStructField(output.getStreamFieldName, ComputeDataType.fromStructField(output.getFieldType), true)
    })
    val schema2 = DataTypes.createStructType(structFields.toArray)

    val transformedRDD = rdd.mapPartitions {
      iterator => {
        val list = iterator.toList
        list.map {
          x => {
            lazy val schema = new Schema.Parser().parse(avroSchema)
            lazy val recordInjection: Injection[GenericRecord, Array[Byte]] = GenericAvroCodecs.toBinary(schema)
            val value = recordInjection.invert(x.value().toString.getBytes()).get
            value
          }
        }
      }.iterator
    }.map(_.toString)

    import spark.implicits._

    val df = spark.read
      .schema(schema2)
      .json(spark.createDataset(transformedRDD))

    df.show()

    df
  }
}
