package com.scistor.operator

import com.scistor.compute.interfacex.SparkProcessProxy
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.JavaConversions._

class FlatMap extends SparkProcessProxy {
  override def transform(spark: SparkSession, table: Dataset[Row]): Dataset[Row] = {
    import spark.implicits._

    val schema = ScalaReflection.schemaFor[FilterModel].dataType.asInstanceOf[StructType]
    val ds = spark.createDataFrame(table.rdd, schema)
    ds.show()
    val f = ds.as[FilterModel].flatMap {
      case x: FilterModel => {
        var sips: java.util.List[java.lang.Long] = new java.util.ArrayList[java.lang.Long]()
        var dips: java.util.List[java.lang.Long] = new java.util.ArrayList[java.lang.Long]()
        if (StringUtils.isNotBlank(x.sip)) {
          if (x.sip.indexOf("/") >= 0) {
            sips.addAll(IPChange.parseIpMaskRange(x.sip.split("/")(0), x.sip.split("/")(1)))
          } else {
            sips.add(IPChange.getIpFromString(x.sip))
          }
        }
        if (StringUtils.isNotBlank(x.dip)) {
          if (x.dip.indexOf("/") >= 0) {
            dips.addAll(IPChange.parseIpMaskRange(x.dip.split("/")(0), x.dip.split("/")(1)))
          } else {
            dips.add(IPChange.getIpFromString(x.dip))
          }
        }

        if (sips.length > 0 && dips.length > 0) {
          sips.map(m => {
            dips.map(n =>
              FilterModel(m.toString,
                x.sp,
                n.toString,
                x.dp,
                ConstantsUtil.reservedCountryMap.getOrDefault(x.src_c, ""),
                ConstantsUtil.reservedCountryMap.getOrDefault(x.dst_c, ""),
                x.ln1)
            )
          })
        } else if (sips.length > 0 && dips.length == 0) {
          sips.map(m => {
            Seq(1).map(n => FilterModel(m.toString,
              x.sp,
              x.dip,
              x.dp,
              ConstantsUtil.reservedCountryMap.getOrDefault(x.src_c, ""),
              ConstantsUtil.reservedCountryMap.getOrDefault(x.dst_c, ""),
              x.ln1))
          })
        } else if (sips.length == 0 && dips.length > 0) {
          dips.map(m => {
            Seq(1).map(n => FilterModel(x.sip,
              x.sp,
              m.toString,
              x.dp,
              ConstantsUtil.reservedCountryMap.getOrDefault(x.src_c, ""),
              ConstantsUtil.reservedCountryMap.getOrDefault(x.dst_c, ""),
              x.ln1))
          })
        } else {
          Seq(1).map(m => {
            Seq(1).map(n => FilterModel(x.sip,
              x.sp,
              x.dip,
              x.dp,
              ConstantsUtil.reservedCountryMap.getOrDefault(x.src_c, ""),
              ConstantsUtil.reservedCountryMap.getOrDefault(x.dst_c, ""),
              x.ln1))
          })
        }
      }
    }.flatMap(x => x)

    val filterDF: DataFrame = f.toDF()

    filterDF

  }
}
