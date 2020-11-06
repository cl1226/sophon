package org.apache.spark.sql

import java.io.{BufferedWriter, OutputStreamWriter, PrintWriter}
import java.lang.reflect.{Method, Modifier}

import com.scistor.compute.interfacex.{ComputeOperator, SparkProcessProxy}
import com.scistor.compute.model.remote.{OperatorImplementMethod, StreamFieldDTO, TransStepDTO}
import com.scistor.compute.model.spark._
import com.scistor.compute.until.{ClassCreateUtils, ConstantUtils}
import com.scistor.compute.utils.UdfUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericRowWithSchema}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{AtomicType, ComputeDataType, DataTypes, StructField, StructType, ssfunctions}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object ComputeProcess {

  def executeJavaProcess(spark: SparkSession,
                         df: DataFrame,
                         operator: ComputeOperator,
                         step: TransStepDTO): DataFrame = {
    val oldSchema = df.schema

    val inputs = step.getInputFields.asScala.map(_.getStreamFieldName)
    val inFieldMap: mutable.Map[String, StreamFieldDTO] = new mutable.HashMap[String, StreamFieldDTO]()
    val inStreamFieldMap: mutable.Map[String, StreamFieldDTO] = new mutable.HashMap[String, StreamFieldDTO]()
    step.getInputFields.asScala.map(f => {
      inFieldMap += (f.getFieldName -> f)
      inStreamFieldMap += (f.getStreamFieldName -> f)
    })

    val outputs = step.getOutputFields.asScala.map(_.getStreamFieldName)
    val outFieldMap: mutable.Map[String, StreamFieldDTO] = new mutable.HashMap[String, StreamFieldDTO]()
    val outStreamFieldMap: mutable.Map[String, StreamFieldDTO] = new mutable.HashMap[String, StreamFieldDTO]()
    step.getOutputFields.asScala.map(f => {
      outFieldMap += (f.getFieldName -> f)
      outStreamFieldMap += (f.getStreamFieldName -> f)
    })

    var outSchema = new StructType()
    step.getOutputFields.asScala.foreach(out => {
      outSchema = outSchema.add(out.getFieldName, ComputeDataType.fromStructField(out.getFieldType), true)
    })
    df.schema.foreach(field => {
      if (!outSchema.exists(_.name.equals(field.name))) outSchema = outSchema.add(field)
    })
    val rddRow = df.rdd.map[Row](row => {
      val rowdata = row.getValuesMap(inputs.toArray[String]).map(data => {
        val field = new ComputeField(data._1, DataType.valueOf(inStreamFieldMap.get(data._1).get.getFieldType.toUpperCase()), true)

        val mapField = inStreamFieldMap.get(data._1).get
        mapField.getConstant.booleanValue() match {
          case false => (mapField.getFieldName, data._2)
          case true => {
            val structField = StructField(mapField.getFieldName, ComputeDataType.fromStructField(field.getDataType.name()))
            (mapField.getFieldName, ConstantUtils.castConstant(structField, mapField.getConstantValue))
          }
        }
      })

      val map = new mutable.HashMap[String, AnyRef]() ++= (rowdata.asInstanceOf[Map[String, AnyRef]])
      val resultProcess = operator.process(map.asJava, packageAttrByStep(step)).map(data => {
        var field: ComputeField = null
        var outKey: StreamFieldDTO = null
        println(s"data._1: ${data._1}")
        if(outStreamFieldMap.containsKey(data._1)) {
          field = new ComputeField(data._1, DataType.valueOf(outStreamFieldMap.get(data._1).get.getFieldType.toUpperCase()), true)
          outKey = outStreamFieldMap.get(data._1).get
        }

        if (outKey != null && outKey.getConstant == false) {
          (outKey.getFieldName, data._2)
        } else if (outKey != null && outKey.getConstant == true) {
          val structField = StructField(field.getFieldName, ComputeDataType.fromStructField(field.getDataType.name()))
          (outKey.getFieldName, ConstantUtils.castConstant(structField, outKey.getConstantValue))
        } else {
          data
        }
      })
      val resData: ArrayBuffer[Any] = ArrayBuffer()
      outSchema.foreach(field => {
        val oldVal: Any = if (oldSchema.getFieldIndex(field.name).isDefined) row.getAs(field.name) else null
        if (resultProcess.get(field.name).isDefined) resData += resultProcess.get(field.name).get else resData += oldVal
      })
      new GenericRowWithSchema(resData.toArray, outSchema)
    })
    val frame: DataFrame = spark.createDataFrame(rddRow, outSchema)
    frame
  }

  /**
   * process spark jar(single dataset).
   **/
  def executeSparkProcess(session: SparkSession,
                          df: DataFrame,
                          process: SparkProcessProxy,
                          step: TransStepDTO): DataFrame = {
    var res = df
    step.getInputFields.foreach(f => {
      f.getConstant.booleanValue() match {
        case false => {
          if (!f.getFieldName.equals(f.getStreamFieldName)) {
            res = res.withColumn(f.getFieldName, col(s"${f.getStreamFieldName}"))
          }
        }
        case true => {
          val structField = StructField(f.getFieldName, ComputeDataType.fromStructField(f.getFieldType))
          res = res.withColumn(f.getFieldName, functions.lit(ConstantUtils.castConstant(structField, f.getConstantValue)))
        }
      }
    })
    res = process.transform(session, res)

    step.getOutputFields.foreach(f => {
      f.getConstant.booleanValue() match {
        case false => {
          if (!f.getFieldName.equals(f.getStreamFieldName)) {
            res = res.withColumn(f.getFieldName, col(s"${f.getStreamFieldName}"))
          }
        }
        case true => {
          val structField = StructField(f.getFieldName, ComputeDataType.fromStructField(f.getFieldType))
          res = res.withColumn(f.getFieldName, functions.lit(ConstantUtils.castConstant(structField, f.getConstantValue)))
        }
      }
    })
    res
  }

  /**
   * process private code.
   **/
  def processPrivate(session: SparkSession, df: DataFrame, invokeInfo: InvokeInfo): DataFrame = {
    import session.sqlContext.implicits._
    var res: DataFrame = null
    val params = invokeInfo.getProcessField.map(field => {
      val sfd = ComputeDataType.fromStructFieldJson(field.getStructFieldJson)
      field.isConstant match {
        case true => ConstantUtils.castConstant(sfd, field.getConstantValue)
        case false => $"${sfd.name}"
      }
    })
    var method: Method = null
    var instance: Object = null
    val classload = Class.forName(invokeInfo.getClassName)
    try {
      method = classload.getMethod(invokeInfo.getMethodName, params.toArray.map(_.getClass): _*)
    } catch {
      case e: NoSuchMethodException => {
        if (method == null) {
          method = classload.getMethods.find(method => {
            method.getName.equals(invokeInfo.getMethodName) && method.getParameterCount == invokeInfo.getProcessField.size() && {
              val parmamtypes = params.toArray.map(_.getClass)
              val types = method.getParameterTypes
              val isMatch = true
              for (i <- 0 to parmamtypes.size - 1) {
                isMatch && types(i).isAssignableFrom(parmamtypes(i))
              }
              isMatch
            }
          }).get
        }
        val isStatic = Modifier.isStatic(method.getModifiers)
        if (!isStatic)
          instance = classload.getConstructor(classOf[java.lang.String]).newInstance(invokeInfo.getProcessField.get(0).getFieldName).asInstanceOf[Column]
      }
    }
    val args = params.toArray.map(_.asInstanceOf[Object])
    val alias = if (StringUtils.isNoneEmpty(invokeInfo.getAlias)) invokeInfo.getAlias else invokeInfo.getProcessField.get(0).getFieldName

    val resCol = method.invoke(instance, args: _*).asInstanceOf[Column]
    if (df.withColumn("Boolean", resCol).schema.find(_.name.equals("Boolean")).get.dataType.equals(DataTypes.BooleanType)) {
      res = df.filter(resCol)
    } else {
      res = df.withColumn(alias, resCol)
    }
    res
  }

  /**
   * process dynamic code.
   */
  def processDynamicCode(session: SparkSession, df: DataFrame, invokeInfo: InvokeInfo, step: TransStepDTO): DataFrame = {
    var res: DataFrame = df
    val schema = df.schema
    val alias = step.getOutputFields.get(0).getFieldName

    val tmpcol = "scistor_tmp"
    var returnDataType: AtomicType = null
    var method: Method = null

    OperatorImplementMethod.get(step.getStepType) match {
      case OperatorImplementMethod.ScriptJava => {

        var columns = df.columns.map(df.col(_)).toBuffer
        if (invokeInfo.getCode != null && !"".equals(invokeInfo.getCode)) {

          val args: Seq[Expression] = {
            val objects = step.getInputFields.map(f => {
              if (f.getConstant) {
                functions.lit(f.getConstantValue).expr
              } else {
                functions.col(f.getStreamFieldName).expr
              }
            })
            objects
          }
          // 单参数返回
          columns = columns.filter(i => !i.named.name.equals(alias))
          columns += ssfunctions.codeInvoke(invokeInfo.getCode, invokeInfo.getMethodName, args: _*).as(alias)

          // 多参数返回
//          columns = columns.filter(m => {
//            var flag = true
//            val booleans = invokeInfo.getProcessField.filter(n => m.named.name.equals(n.getFieldName))
//            flag = !(booleans.length > 0)
//            flag
//          })
//          var i: Int = 0
//          invokeInfo.getProcessField.map(x => {
//            val code = invokeInfo.getCode
//              .replace("[ReturnBlock]", "return map.get(\"" + invokeInfo.getAliasField.get(i)(1) + "\")")
//              .replace("[ReturnType]", "Object")
//            columns += ssfunctions.codeInvoke(code, invokeInfo.getMethodName, args: _*).as(invokeInfo.getAliasField.get(i)(0))
//            i += 1
//          })

          res = df.select(columns: _*)
        } else {
          val args = {
            val objects = invokeInfo.getProcessField.map(filed => {
              if (filed.isConstant) "'" + filed.getConstantValue + "'" else filed.getFieldName
            })
            objects
          }.mkString(",")
          res = df.selectExpr("*", s"""java_method('${invokeInfo.getClassName}', '${invokeInfo.getMethodName}', $args)""")
        }
        res
      }

      case OperatorImplementMethod.ScriptScala => {
        method = ClassCreateUtils.apply(invokeInfo.getCode).methods(invokeInfo.getMethodName)
        returnDataType = UdfUtils.convertSparkType(method.getReturnType.getName)
        val newschema = schema.add(tmpcol, returnDataType)
        val afterRdd = df.rdd.map[Row](row => {
          val params = invokeInfo.getProcessField.map(field => {
            val sfd = ComputeDataType.fromStructFieldJson(field.getStructFieldJson)
            field.isConstant match {
              case true => ConstantUtils.castConstant(sfd, field.getConstantValue)
              case false => row.getAs(sfd.name)
            }
          })
          val args = params.toArray.map(_.asInstanceOf[Object])
          val result = ClassCreateUtils.apply(invokeInfo.getCode).invoke[Any](args.toSeq)
          val buffer = row.toSeq.toBuffer
//          buffer(row.fieldIndex(alias)) = result
//          new GenericRowWithSchema(buffer.toArray, schema)
          buffer += result
          new GenericRowWithSchema(buffer.toArray, newschema)
        })
        val frame = session.createDataFrame(afterRdd, newschema)
        res = frame.withColumn(alias, frame.col(tmpcol))
        res
      }
    }
  }

  /**
   * execute pipeline process
   */
  def pipeLineProcess(session: SparkSession, df: DataFrame, field: String, command: Array[String], alias: String): DataFrame = {
    val resultRdd = df.rdd.mapPartitions(partion => {
      val list = partion.toList

      val pb = new ProcessBuilder(command: _*)
      val process1 = pb.start()

      val out = new PrintWriter(new BufferedWriter(
        new OutputStreamWriter(process1.getOutputStream, "utf-8")))

      list.foreach(row => {
        val str = row.getAs[String](field)
        out.println(str)
      })
      out.flush()
      out.close()

      val lines = Source.fromInputStream(process1.getInputStream)("utf-8").getLines

      val newPartition = list.iterator.map[Row](row => {
        val str = row.getAs[String](field)
        out.println(str)
        out.flush()

        val value1 = row.toSeq.toBuffer
        value1(row.fieldIndex(field)) = lines.next()
        new GenericRowWithSchema(value1.toArray, row.schema)
      })
      out.close()

      def updateRow(row: Row, it: Iterator[String]): Row = {
        val value1 = row.toSeq.toBuffer
        var res: GenericRowWithSchema = null
        if (it.hasNext()) {
          val str = it.next()
          value1(row.fieldIndex(field)) = str
          res = new GenericRowWithSchema(value1.toArray, row.schema)
        } else {
          updateRow(row, it)
        }
        res
      }

      newPartition
    })

    val res = session.createDataFrame(resultRdd, df.schema)
    res.withColumn(alias, res.col(field))
  }

  def packageAttrByStep(step: TransStepDTO): java.util.Map[java.lang.String, java.lang.String] = {
    val res = new java.util.HashMap[java.lang.String, java.lang.String]()
    res.put(ComputeOperator.OPERATOR_TYPE, step.getStepType)
    res.put(ComputeOperator.STEP_NAME, step.getStepId)

    step.getStepAttributes.asScala.map(x => {
      res.put(x._1, x._2.asInstanceOf[String])
    })

    res
  }

}
