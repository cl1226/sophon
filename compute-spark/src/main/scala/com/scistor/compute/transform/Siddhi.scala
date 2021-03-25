package com.scistor.compute.transform

import com.alibaba.fastjson.JSON
import com.scistor.compute.SparkJobStarter.siddhiAppRuntime
import com.scistor.compute.apis.BaseTransform
import com.scistor.compute.model.remote.{SparkStepDTO, TransStepDTO}
import io.siddhi.core.SiddhiManager
import io.siddhi.core.event.Event
import io.siddhi.core.stream.output.StreamCallback
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ComputeDataType, DataTypes, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class Siddhi extends BaseTransform {

  var config: TransStepDTO =_

  /**
   * Set Config.
   * */
  override def setConfig(config: TransStepDTO): Unit = {
    this.config = config
  }

  /**
   * Get Config.
   * */
  override def getConfig(): TransStepDTO = config

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {
    val attrs = config.getStepAttributes

//    println(s"[INFO] 转换算子 [${config.getStepType}] properties: ")
//    attrs.foreach(entry => {
//      val (key, value) = entry
//      println("\t" + key + " = " + value)
//    })

    val stepList: java.util.List[SparkStepDTO] = JSON.parseArray(attrs.get("stepList").toString, classOf[SparkStepDTO])

    val inStreams = stepList.filter(_.getStepInfo.getStepType.equals("in_stream"))
    val outStreams = stepList.filter(_.getStepInfo.getStepType.equals("out_stream"))

    val inStreamName = inStreams.get(0).getStepInfo.getName
    val outStreamName = outStreams.get(0).getStepInfo.getName

    val inputFields = inStreams.get(0).getStepInfo.getInputFields

    val columns = inputFields.map(in => {
      col(in.getFieldName).cast(ComputeDataType.fromStructField(in.getFieldType))
    })

    val fromDF = df.select(columns:_*)

    var tempArray = ArrayBuffer.empty[AnyRef]
    siddhiAppRuntime.addCallback(outStreamName, new StreamCallback() {
      override def receive(events: Array[Event]): Unit = {
        for (e <- events) {
          val data = e.getData.mkString(",")
          tempArray+=data
        }
      }
    })
    siddhiAppRuntime.start()

    val fields = fromDF.schema.fields
    fromDF.collect().map(r => {
      val inputHandler = siddhiAppRuntime.getInputHandler(inStreamName)
      val split = r.mkString(",").split(",")
      val objects = new Array[Any](r.length)

      for (i <- 0 until split.length) {
        val typeName = fields(i).dataType.typeName.toLowerCase
        var v: Any = null
        typeName match {
          case "string" => v = split(i)
          case "int" | "integer" => v = java.lang.Integer.valueOf(split(i))
          case "long" => v = java.lang.Long.valueOf(split(i))
          case "float" => v = java.lang.Float.valueOf(split(i))
          case "double" => v = java.lang.Double.valueOf(split(i))
          case _ => v = split(i)
        }
        objects(i) = v
      }
      inputHandler.send(objects.asInstanceOf[Array[Object]])
    })

    val outputFields = outStreams.get(0).getStepInfo.getOutputFields
    val outputStructFields = mutable.ArrayBuffer[StructField]()
    outputFields.foreach(output => {
      outputStructFields += DataTypes.createStructField(output.getFieldName, ComputeDataType.fromStructField(output.getFieldType), true)
    })
    val resultSchema = DataTypes.createStructType(outputStructFields.toArray)

    val rdd = tempArray.map(v => {
      val split = v.toString.split(",")
      val objects = new Array[Any](split.length)
      for (i <- 0 until split.length) {
        val typeName = outputFields(i).getFieldType.toLowerCase
        var temp: Any = null
        typeName match {
          case "string" => temp = split(i)
          case "int" | "integer" => temp = java.lang.Integer.valueOf(split(i))
          case "long" => temp = java.lang.Long.valueOf(split(i))
          case "float" => temp = java.lang.Float.valueOf(split(i))
          case "double" => temp = java.lang.Double.valueOf(split(i))
          case _ => temp = split(i)
        }
        objects(i) = temp
      }

      Row.fromSeq(objects)
    })
    spark.createDataFrame(rdd, resultSchema)
  }

  /**
   * Return true and empty string if config is valid, return false and error message if config is invalid.
   */
  override def validate(): (Boolean, String) = (true, "")
}
