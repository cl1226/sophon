package com.scistor.compute.transform

import com.alibaba.fastjson.JSON
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

    val siddhiApp: String = attrs.getOrDefault("text", "").toString
    val stepList: java.util.List[SparkStepDTO] = JSON.parseArray(attrs.get("stepList").toString, classOf[SparkStepDTO])

    val inStreams = stepList.filter(_.getStepInfo.getStepType.equals("in_stream"))
    val outStreams = stepList.filter(_.getStepInfo.getStepType.equals("out_stream"))

    var result: RDD[AnyRef] = null

    val inStreamName = inStreams.get(0).getStepInfo.getName
    val outStreamName = outStreams.get(0).getStepInfo.getName

    val inputFields = inStreams.get(0).getStepInfo.getInputFields

    var array = Array.empty[String]
    inputFields.foreach(input => {
      array = array :+ input.getFieldName
    })

    val fromDF = df.select(array.map(col(_)):_*)
    val fields = fromDF.schema.fields
    result = fromDF.rdd.mapPartitions(part => {
      val siddhiManager = new SiddhiManager
      val runtime = siddhiManager.createSiddhiAppRuntime(siddhiApp)

      var tempArray = ArrayBuffer.empty[AnyRef]
      runtime.addCallback(outStreamName, new StreamCallback() {
        override def receive(events: Array[Event]): Unit = {
          for (e <- events) {
            val data = e.getData.mkString(",")
            tempArray+=data
          }
        }
      })

      runtime.start()

      val inputHandler = runtime.getInputHandler(inStreamName)
      part.foreach(r => {

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
      tempArray.toIterator
    })

    val outputFields = outStreams.get(0).getStepInfo.getOutputFields
    val outputStructFields = mutable.ArrayBuffer[StructField]()
    outputFields.foreach(output => {
      outputStructFields += DataTypes.createStructField(output.getFieldName, ComputeDataType.fromStructField(output.getFieldType), true)
    })
    val resultSchema = DataTypes.createStructType(outputStructFields.toArray)

    val rdd = result.map(v => {
      Row.fromSeq(v.toString.split(","))
    })
    spark.createDataFrame(rdd, resultSchema)
  }

  /**
   * Return true and empty string if config is valid, return false and error message if config is invalid.
   */
  override def validate(): (Boolean, String) = (true, "")
}
