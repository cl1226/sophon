package com.scistor.compute.interfacex

import java.util

import com.scistor.compute.model.spark.ComputeField
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

trait TableProcess extends Serializable {
//  val processTabFunc = processTable _
  def process(data: java.util.Map[String, Object]): java.util.Map[String, Object]
}

abstract class TableProcessProxy extends TableProcess{
  def process(
    data: util.Map[String,Object]): util.Map[String, Object]

  private[compute] def processTable(spark: SparkSession, table: Dataset[Row], newTableSchme: StructType): Dataset[Row] = {
    implicit val matchError = org.apache.spark.sql.Encoders.kryo[Row]

    val schema = table.schema
    val newTable = table.rdd.map[Option[Row]](row=>{
      val rowfields = row.schema.toList.map(s=>s.name)
      val tableData = row.getValuesMap(rowfields)

      val res:util.Map[String, Object] = process(tableData.asJava)
      if(res == null || res.size() == 0){
        Option.empty[Row]
      }else {
        val rowSchme = newTableSchme.toList.map(s=>s.name)
        val buffer = ArrayBuffer[Any]()
        rowSchme.foreach(field => {
          buffer += res.getOrDefault(field,null)
        })
        //      new GenericRow(buffer.toArray)
        Option.apply(new GenericRowWithSchema(buffer.toArray,newTableSchme))
      }
    }).filter(_.isDefined).map(_.get)

    val result = spark.createDataFrame(newTable, newTableSchme)
    result
  }
}

object TableProcess extends TableProcess {
  private val fields: ArrayBuffer[ComputeField] = ArrayBuffer()


  def addTableSchme(field: ComputeField): Unit = {
    fields += field
  }

  def setTableSchme(fields: ComputeField): Unit = {
    //    fields += field
  }

  def computeFieldToStructField(computeField: ComputeField): StructField ={
    new StructField(computeField.getFieldName,computeField.getDataType.getSparkDataType())
  }

  def getTableSchme(fields: ArrayBuffer[ComputeField]): StructField ={
    val structType = new StructType()
    fields.foreach(field => {
      structType.add(computeFieldToStructField(field))
    })
    return null;
  }

  override def process(
    data: util.Map[String,
                               Object]
  ): util.Map[String, Object] =
    ???
}