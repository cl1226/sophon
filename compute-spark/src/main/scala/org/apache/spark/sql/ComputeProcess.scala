package org.apache.spark.sql

import java.io.{BufferedWriter, OutputStreamWriter, PrintWriter}
import java.lang.reflect.{Method, Modifier}
import java.sql

import com.scistor.compute.interfacex.{ComputeOperator, SparkProcessProxy, SparkProcessProxy2}
import com.scistor.compute.model.remote.{OperatorImplementMethod, TransStepDTO}
import com.scistor.compute.model.spark._
import com.scistor.compute.until.{ClassCreateUtils, CompileUtils, ConstantUtils}
import com.scistor.compute.utils.UdfUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericRowWithSchema}
import org.apache.spark.sql.types.{AtomicType, ComputeDataType, DataTypes, StructType, ssfunctions}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object ComputeProcess {

  /**
   * Process java jar.
   **/
  def computeOperatorProcess(
                              session: SparkSession,
                              df: DataFrame,
                              defindmethod: UserDefineOperator,
                              operator: ComputeOperator,
                              step: TransStepDTO): DataFrame = {

    val inputs = defindmethod.getInputMapping.asScala.map(_._1)
    val outputs = defindmethod.getOutputMapping.asScala.map(_._2.getFieldName)

    var outSchme = new StructType()
    //df.schema
    val oldSchme = df.schema
    println(s"execute java jar: ")
    defindmethod.getOutputMapping.asScala.foreach(out => {
      val structField2 = ComputeDataType.fromStructFieldJson(out._2.getStructFieldJson)
      outSchme = outSchme.add(out._2.getFieldName, structField2.dataType, out._2.isNullable)
    })

    df.schema.foreach(field => {
      if (!outSchme.exists(_.name.equals(field.name))) outSchme = outSchme.add(field)
    })

    val rddRow = df.rdd.map[Row](row => {
      //get Table exits data and rename
      val rowdata = row.getValuesMap(inputs.toArray[String]).map(data => {
        val field = new ComputeField(data._1, DataType.STRING, true)
        val mapField = defindmethod.getInputMapping.get(field.getFieldName)

        defindmethod.getInputMapping.get(field.getFieldName).isConstant match {
          case false => {
            (mapField.getFieldName, data._2)
          }
          case true => (mapField.getFieldName, ConstantUtils.castConstant(ComputeDataType.fromStructFieldJson(mapField.getStructFieldJson), mapField.getConstantValue))
        }
      })

      //get outdata and rename
      val map = new mutable.HashMap[String, AnyRef]() ++= (rowdata.asInstanceOf[Map[String, AnyRef]])
      var resultProcess = operator.process(map.asJava, packageAttrByStep(step)).map(data => {
        val field = new ComputeField(data._1, DataType.STRING, true)
        val outKey = defindmethod.getOutputMapping.get(field.getFieldName)

        if (outKey != null && outKey.isConstant == false) {
          (outKey.getFieldName, data._2)
        } else if (outKey != null && outKey.isConstant == true) {
          (outKey.getFieldName, ConstantUtils.castConstant(ComputeDataType.fromStructFieldJson(outKey.getStructFieldJson), outKey.getConstantValue))
        } else {
          data
        }
      })

      resultProcess = resultProcess.map(data => {
        if (data._2.isInstanceOf[java.util.Date])
          (data._1.replaceAll("appEmail_", ""), new sql.Date(data._2.asInstanceOf[java.util.Date].getTime).asInstanceOf[Any])
        else if (data._2.isInstanceOf[java.util.Map[Any, Any]])
          (data._1.replaceAll("appEmail_", ""), data._2.asInstanceOf[java.util.Map[Any, Any]].asScala)
        else if (data._2.isInstanceOf[java.util.List[Any]])
          (data._1.replaceAll("appEmail_", ""), data._2.asInstanceOf[java.util.List[Any]].asScala)
        else
          (data._1.replaceAll("appEmail_", ""), data._2)
      })
      val resData: ArrayBuffer[Any] = ArrayBuffer()
      outSchme.foreach(field => {
        val oldVal: Any = if (oldSchme.getFieldIndex(field.name).isDefined) row.getAs(field.name) else null
        if (resultProcess.get(field.name).isDefined) resData += resultProcess.get(field.name).get else resData += oldVal
      })
      new GenericRowWithSchema(resData.toArray, outSchme)
    })
    val frame: DataFrame = session.createDataFrame(rddRow, outSchme)
    frame

  }

  /**
   * process spark jar(single dataset).
   **/
  def computeSparkProcess(session: SparkSession, dfinput: DataFrame, udo: UserDefineOperator, process: SparkProcessProxy): DataFrame = {
    var res = dfinput
    udo.getInputMapping.foreach(mapping => {
      mapping._2.isConstant match {
        case false => res = res.withColumnRenamed(mapping._1, mapping._2.getFieldName)
        case true => res = res.withColumn(mapping._2.getFieldName, functions.lit(ConstantUtils.castConstant(ComputeDataType.fromStructFieldJson(mapping._2.getStructFieldJson), mapping._2.getConstantValue)))
      }
    })
    res = process.transform(session, res)

    udo.getOutputMapping.foreach(mapping => {
      mapping._2.isConstant match {
        case false => res = res.withColumnRenamed(mapping._1, mapping._2.getFieldName)
        case true => res = res.withColumn(mapping._2.getFieldName, functions.lit(ConstantUtils.castConstant(ComputeDataType.fromStructFieldJson(mapping._2.getStructFieldJson), mapping._2.getConstantValue)))
      }
    })
    res
  }

  /**
   * process spark jar(two datasets).
   **/
  def computeSparkProcess2(session: SparkSession, dfinput: DataFrame, udo: UserDefineOperator, process: SparkProcessProxy2): DataFrame = {
    var res = dfinput
    udo.getInputMapping.foreach(mapping => {
      mapping._2.isConstant match {
        case false => res = res.withColumnRenamed(mapping._1, mapping._2.getFieldName)
        case true => res = res.withColumn(mapping._2.getFieldName, functions.lit(ConstantUtils.castConstant(ComputeDataType.fromStructFieldJson(mapping._2.getStructFieldJson), mapping._2.getConstantValue)))
      }
    })
    res = process.transform(session, res, res)

    udo.getOutputMapping.foreach(mapping => {
      mapping._2.isConstant match {
        case false => res = res.withColumnRenamed(mapping._1, mapping._2.getFieldName)
        case true => res = res.withColumn(mapping._2.getFieldName, functions.lit(ConstantUtils.castConstant(ComputeDataType.fromStructFieldJson(mapping._2.getStructFieldJson), mapping._2.getConstantValue)))
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
    val alias = if (StringUtils.isNoneEmpty(invokeInfo.getAlias)) invokeInfo.getAlias else invokeInfo.getProcessField.get(0).getFieldName

    val tmpcol = "scistor_tmp"
    var returnDataType: AtomicType = null
    var method: Method = null

    OperatorImplementMethod.get(step.getStepType) match {
      case OperatorImplementMethod.ScriptJava => {

        var columns = df.columns.map(df.col(_)).toBuffer
        if (invokeInfo.getCode != null && !"".equals(invokeInfo.getCode)) {

          val args: Seq[Expression] = {
            val objects = invokeInfo.getProcessField.map(filed => {
              if (filed.isConstant) functions.lit(filed.getConstantValue).expr else functions.col(filed.getFieldName).expr
            })
            objects
          }
          // 单参数返回
          columns = columns.filter(i => !i.named.name.equals(invokeInfo.getAlias))
          columns += ssfunctions.codeInvoke(invokeInfo.getCode, invokeInfo.getMethodName, args: _*).as(invokeInfo.getAlias)

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

      val newPartion = list.iterator.map[Row](row => {
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

      newPartion
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
