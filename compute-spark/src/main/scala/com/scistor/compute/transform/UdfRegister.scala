package com.scistor.compute.transform

import java.util.ServiceLoader

import com.scistor.compute.apis.BaseTransform
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

object UdfRegister {

  def findAndRegisterUdfs(spark: SparkSession): Unit = {

    println("find and register UDFs & UDAFs")

    var udfCount = 0
    var udafCount = 0
    val services = (ServiceLoader load classOf[BaseTransform]).asScala
    services.foreach(f => {

      f.getUdfList()
        .foreach(udf => {
          val (udfName, udfImpl) = udf
          spark.udf.register(udfName, udfImpl)
          udfCount += 1
        })

      f.getUdafList()
        .foreach(udaf => {
          val (udafName, udafImpl) = udaf
          spark.udf.register(udafName, udafImpl)
          udafCount += 1
        })
    })

    println("found and registered UDFs count[" + udfCount + "], UDAFs count[" + udafCount + "]")
  }

}
