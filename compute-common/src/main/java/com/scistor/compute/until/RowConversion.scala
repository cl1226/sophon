package com.scistor.compute.until

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

import scala.collection.mutable

object RowConversion {

  def parseRow2Map(row: Row): java.util.Map[String, AnyRef] ={
    val schema = row.schema.map(_.name)
    val res = new java.util.HashMap[String, AnyRef]
    row.getValuesMap(schema).foreach(v => res.put(v._1, ParseValue(v._2)))
    res
  }

  def ParseValue(value: AnyRef): AnyRef ={
    var res: AnyRef = null
    if(value.isInstanceOf[mutable.WrappedArray[AnyRef]]){
      val list = new java.util.ArrayList[AnyRef]()
      value.asInstanceOf[mutable.WrappedArray[AnyRef]].foreach(v => list.add(ParseValue(v)))
      res = list
    }else if(value.isInstanceOf[GenericRowWithSchema]){
      res = parseRow2Map(value.asInstanceOf[GenericRowWithSchema])
    }else {
      res = value
    }
    res
  }
}
