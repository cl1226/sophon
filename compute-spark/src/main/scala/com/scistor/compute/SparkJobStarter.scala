package com.scistor.compute

import com.alibaba.fastjson.JSON
import com.scistor.compute.apis.{BaseOutput, BaseStaticInput, BaseStreamingInput, Plugin}
import com.scistor.compute.interfacex.{ComputeOperator, SparkProcessProxy, SparkProcessProxy2}
import com.scistor.compute.model.portal.JobApiDTO
import com.scistor.compute.model.spark.{ComputeAttribute, ComputeJob, ComputeSinkAttribute, OperatorType, ProjectInfo}
import com.scistor.compute.transform.UdfRegister
import com.scistor.compute.utils.CommonUtil.writeSimpleData
import com.scistor.compute.utils.JobInfoTransfer.jedis
import com.scistor.compute.utils.{AsciiArt, ClassUtils, CommonUtil, ConfigBuilder, JdbcUtil, JobInfoTransfer}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.ComputeProcess.{computeOperatorProcess, computeSparkProcess, computeSparkProcess2, pipeLineProcess, processDynamicCode, processPrivate}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalaj.http
import scalaj.http.Http

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.util.control.Breaks.{break, breakable}

object SparkJobStarter extends Logging {

  var viewTableMap: Map[String, Dataset[Row]] = Map[String, Dataset[Row]]()

  var jobName: String = _

  def main(args: Array[String]): Unit = {

    if (args.length == 0) {
      logError("请求参数列表为空!")
      System.exit(-1)
    }

    if (args(0).indexOf("http") < 0) {
      logError("请求参数非法!")
      System.exit(-1)
    }

    logInfo(s"${args(0)}")

    val response: http.HttpResponse[String] = Http(args(0)).header("Accept", "application/json").timeout(10000, 1000).asString

    val jobApiDTO: JobApiDTO = JSON.parseObject(response.body, classOf[JobApiDTO])

    val projectInfo = JobInfoTransfer.portalModel2SparkModel(jobApiDTO)

    val sparkConf = createSparkConf()
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    entrypoint(projectInfo)

  }

  private def entrypoint(info: ProjectInfo) = {
    val configBuilder = new ConfigBuilder
    println("[INFO] loading SparkConf: ")
    val sparkConf = createSparkConf()
    sparkConf.getAll.foreach(entry => {
      val (key, value) = entry
      println("\t" + key + " => " + value)
    })

    val sparkSession = SparkSession.builder.config(sparkConf).getOrCreate()

    // find all user defined UDFs and register in application init
    UdfRegister.findAndRegisterUdfs(sparkSession)

    val staticInputs = configBuilder.createStaticInputs("batch")
    val streamingInputs = configBuilder.createStreamingInputs("batch")
    val outputs = configBuilder.createOutputs[BaseOutput]("batch")

    if (streamingInputs.nonEmpty) {
      streamingProcessing(sparkSession, staticInputs, streamingInputs, outputs, info)
    } else {
      batchProcessing(sparkSession, staticInputs, outputs, info)
    }
  }

  /**
   * Batch Processing
   **/
  private def batchProcessing(sparkSession: SparkSession,
                              staticInputs: List[BaseStaticInput],
                              outputs: List[BaseOutput],
                              info: ProjectInfo): Unit = {
    jobName = sparkSession.sparkContext.getConf.get("spark.app.name", info.getProjectName)

    prepare(sparkSession, info)

    basePrepare(sparkSession, staticInputs, outputs)

    // when you see this ASCII logo, scistor compute platform is really started.
    showScistorAsciiLogo()

    if (staticInputs.nonEmpty) {
      registerInputTempViewWithHead(staticInputs, sparkSession, info)

      info.getRunJobs.asScala.foreach(step => {
        breakable {
          val df = viewTableMap.get(step.getDataSource)
          if(!df.isDefined) break
          transform(df.get, step, sparkSession, info)
          outputs.remove(step.getJobName)
        }
      })

      outputs.foreach(output => {
        sink(sparkSession, output, info, null)
      })

      sparkSession.stop()

    } else {
      throw new RuntimeException("Input must be configured at least once.")
    }

    sparkSession.stop()

  }

  /**
   * Streaming Processing
   */
  private def streamingProcessing(sparkSession: SparkSession,
                                  staticInputs: List[BaseStaticInput],
                                  streamingInputs: List[BaseStreamingInput[Any]],
                                  outputs: List[BaseOutput],
                                  info: ProjectInfo
                                 ): Unit = {

    jobName = sparkSession.sparkContext.getConf.get("spark.app.name", info.getProjectName)
    val duration = sparkSession.sparkContext.getConf.getLong("spark.streaming.batchDuration", 10L)
    val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(duration))

    basePrepare(sparkSession, staticInputs, streamingInputs, outputs)

    // let static input register as table for later use if needed
    registerInputTempView(staticInputs, sparkSession)
    // when you see this ASCII logo, scistor is really started.
    showScistorAsciiLogo()

    val streamingInput = streamingInputs(0)
    streamingInput.start(
      sparkSession,
      ssc,
      dataset => {
        var ds = dataset

        if (ds.count() > 0) {
          // 写入mysql，统计输入量
          new JdbcUtil(sparkSession, info.getMysqlConfig).writeDataCount("0", ds.count().toString, jobName)

          val tableName = info.getAttribute.sourcenamespace.split("\\.")(2)
          if (info.getAttribute.isInstanceOf[ComputeAttribute]) {
            info.getAttribute.asInstanceOf[ComputeAttribute].getOutputMapping.foreach(out => {
              ds = ds.withColumnRenamed(out._1, out._2.getFieldName)
            })
          }
          viewTableMap += (tableName -> ds)
          ds.persist(StorageLevel.MEMORY_AND_DISK)

          info.getRunJobs.asScala.foreach(step => {
            ds = transform(ds, step, sparkSession, info)
          })
          ds.unpersist()

          if (info.isGetSchema) System.exit(1)

          outputs.foreach(output => {
            sink(sparkSession, output, info, ds)
          })
        } else {
          logInfo(s"${info.getAttribute.getGroupid} consumer 0 record!")
        }
      }
    )
    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * Return Head Static Input DataSet
   */
  private[scistor] def registerInputTempViewWithHead(staticInputs: List[BaseStaticInput],
                                                     sparkSession: SparkSession,
                                                     info: ProjectInfo): Unit = {
    if (staticInputs.nonEmpty) {
      for (input <- staticInputs.slice(0, staticInputs.length)) {
        val ds = input.getDataset(sparkSession)
        convertDataType(ds, info)
        registerInputTempView(input, ds)
        // 写入mysql，统计输入量
        new JdbcUtil(sparkSession, info.getMysqlConfig).writeDataCount("0", ds.count().toString, jobName)
      }
    } else {
      throw new RuntimeException("You must set static input plugin at least once.")
    }
  }

  private[scistor] def showScistorAsciiLogo(): Unit = {
    AsciiArt.printAsciiArt("Scistor")
  }

  private[scistor] def basePrepare(sparkSession: SparkSession, plugins: List[Plugin]*): Unit = {
    for (pluginList <- plugins) {
      for (p <- pluginList) {
        p.prepare(sparkSession)
      }
    }
  }

  private[scistor] def registerInputTempView(staticInputs: List[BaseStaticInput],
                                             sparkSession: SparkSession): Unit = {
    for (input <- staticInputs) {
      val ds = input.getDataset(sparkSession)
      registerInputTempView(input, ds)
    }
  }

  private[scistor] def registerInputTempView(input: BaseStaticInput, ds: Dataset[Row]): Unit = {
    val tableName = input.getSource().getSourcenamespace.split("\\.")(2)
    registerTempView(tableName, ds)
  }

  private[scistor] def registerTempView(tableName: String, ds: Dataset[Row]): Unit = {
    ds.createOrReplaceTempView(tableName)
    viewTableMap += (tableName -> ds)
  }

  /**
   * Convert data source field type to spark schema dataType
   */
  private[scistor] def convertDataType(ds: Dataset[Row], info: ProjectInfo): Unit = {
    // 数据源schema dataType -> spark schema dataType
    info.getAttribute.asInstanceOf[ComputeAttribute].getOutputMapping.foreach(field => {
      if (ds.columns.contains(field._2.getFieldName)) {
        val dataType = field._2.getDataType.getSparkDataType
        ds.withColumn(field._2.getFieldName, ds.col(field._2.getFieldName).cast(dataType))
      }
    })
  }

  private[scistor] def transform(dfinput: DataFrame, step: ComputeJob, session: SparkSession, job: ProjectInfo): DataFrame = {
    dfinput.createOrReplaceTempView(step.getDataSource)
    jedis.set(s"""${jobName}-${step.getJobName}-STATUS""", "RUNNING")

    var df = dfinput

    // execute java/spark jar
    step.getUdos.asScala.foreach(udo => {
      session.sparkContext.addJar(udo.getJarPath)
      val operator = ClassUtils.getUserOperatorImpl(udo)
      operator match {
        // process user define java jar(map function)
        case operator: ComputeOperator => df = computeOperatorProcess(session, df, udo, operator, step)
        // process user define spark jar(single dataset)
        case dfProcess: SparkProcessProxy => df = computeSparkProcess(session, df, udo, dfProcess)
        // process user define spark jar(two dataset)
        case df2Process: SparkProcessProxy2 => df = computeSparkProcess2(session, df, udo, df2Process)
        // other
        case _ => throw new RuntimeException(s"Unsupported define operator: [${udo.getClassFullName}], please check it!")
      }
    })

    // execute java/scala code
    step.getProcess.asScala.foreach(invokeInfo => {
      step.getOperatorType match {
        case OperatorType.PRIVATE => {
          df = processPrivate(session, df, invokeInfo)
        }
        case OperatorType.JAVA | OperatorType.SCALA => {
          df = processDynamicCode(session, df, invokeInfo, step)
        }
        case _ => throw new RuntimeException(s"Unsupported operator type: [${step.getOperatorType}], please check it!")
      }
    })

    // execute sql
    step.getProcessSql.foreach(processSQL => {
      df = session.sql(processSQL)
    })

    // execute python/shell
    step.getCommand.asScala.foreach(command => {
      df = pipeLineProcess(session, df, command._1, command._2.toArray[String](Array[String]()), step.getCommandMaping.get(command._1))
    })

    registerTempView(step.getDataSource, df)
    registerTempView(step.getJobName, df)

    jedis.set(s"""${jobName}-${step.getJobName}-STATUS""", "FINISHED")
    CommonUtil.writeSimpleData(df, job, s"${jobName}-${step.getJobName}")

    df
  }

  private[scistor] def sink(sparkSession: SparkSession,
                            output: BaseOutput,
                            info: ProjectInfo,
                            df: DataFrame): Unit = {
    if (output.getSink().isInstanceOf[ComputeSinkAttribute]) {
      val databaseEnglish = output.getSink().sinknamespace.split("\\.")(2)
      val option = viewTableMap.get(databaseEnglish)
      breakable {
        var ds: Dataset[Row] = null
        if (df == null) {
          if (!option.isDefined) break
          ds = option.get
        } else {
          ds = df
        }

        output.getSink().asInstanceOf[ComputeSinkAttribute].getOutputMapping.foreach(out => {
          if (ds.columns.contains(out._2.getFieldName)) {
            if (!out._2.isConstant) ds = ds.withColumn(out._1, ds.col(out._2.getFieldName))
            else ds = ds.withColumn(out._1, new Column(out._2.getConstantValue))
          }
        })
        val allCols = ds.schema.map(_.name)
        val needCols = output.getSink().asInstanceOf[ComputeSinkAttribute].getOutputMapping.map(_._1)
        allCols.foreach { col =>
          if (!needCols.contains(col)) ds = ds.drop(col)
        }

        println("[INFO] output dataframe: ")
        ds.show(5)

        if (ds.take(1).size > 0) {
          writeSimpleData(ds, info, s"""${jobName}-result""")
        }

        // 写入mysql，统计输出
        new JdbcUtil(sparkSession, info.getMysqlConfig).writeDataCount(ds.count().toString, "0", jobName)
        output.process(ds)
      }
    }
  }

  private[scistor] def prepare(sparkSession: SparkSession, info: ProjectInfo): Unit = {
    info.getRunJobs.asScala.foreach(step => {
      step.getUdos.asScala.foreach(udo => {
        sparkSession.sparkContext.addJar(udo.getJarPath)
      })
    })
  }

  private[scistor] def createSparkConf(): SparkConf = {
    val sparkConf = new SparkConf()
      .set(CATALOG_IMPLEMENTATION.key, "in-memory")
      .set("dfs.client.block.write.replace-datanode-on-failure.policy", "ALWAYS")
      .set("dfs.client.block.write.replace-datanode-on-failure.enable", "true")
      .set("spark.streaming.kafka.maxRatePerPartition", "20000")

    sparkConf.set("spark.master", sparkConf.get("spark.master", "local[*]"))

    sparkConf
  }

}
