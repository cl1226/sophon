package com.scistor.compute

import java.io.File
import java.util

import com.alibaba.fastjson.JSON
import com.scistor.compute.apis.{BaseOutput, BaseStaticInput, BaseStreamingInput, BaseTransform, Plugin}
import com.scistor.compute.model.remote.SparkTransDTO
import com.scistor.compute.model.spark.ProjectInfo
import com.scistor.compute.transform.UdfRegister
import com.scistor.compute.utils.{AsciiArt, CompressionUtils, ConfigBuilder, JdbcUtil, SparkInfoTransfer}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.sql.types.ComputeDataType
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalaj.http
import scalaj.http.Http

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.util.control.Breaks.{break, breakable}
import scala.util.{Failure, Success, Try}

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

    val info = JSON.parseObject(response.body, classOf[SparkTransDTO])

    SparkInfoTransfer.transfer(info)

    Try(entrypoint()) match {
      case Success(_) => {}
      case Failure(exception) => {
        exception match {
          case e: Exception => showRuntimeError(e)
        }
      }
    }

  }

  private def entrypoint() = {
    val configBuilder = new ConfigBuilder
    println("[INFO] loading SparkConf: ")
    val sparkConf = createSparkConf()
    sparkConf.getAll.foreach(entry => {
      val (key, value) = entry
      println("\t" + key + " => " + value)
    })

    val builder = SparkSession.builder.config(sparkConf)
    if (SparkInfoTransfer.enableHiveSupport) builder.enableHiveSupport()
    val sparkSession = builder.getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")

    // find all user defined UDFs and register in application init
    UdfRegister.findAndRegisterUdfs(sparkSession)

    val staticInputs = configBuilder.createStaticInputs("batch")
    val streamingInputs = configBuilder.createStreamingInputs("batch")
    val transforms = configBuilder.createTransforms("batch")
    val outputs = configBuilder.createOutputs[BaseOutput]("batch")

    validate(staticInputs, streamingInputs, transforms, outputs)

    if (streamingInputs.nonEmpty) {
      streamingProcessing(sparkSession, staticInputs, streamingInputs, transforms, outputs)
    } else {
      batchProcessing(sparkSession, staticInputs, transforms, outputs)
    }
  }

  /**
   * Batch Processing
   **/
  private def batchProcessing(sparkSession: SparkSession,
                              staticInputs: List[BaseStaticInput],
                              transforms: List[BaseTransform],
                              outputs: List[BaseOutput]): Unit = {
    jobName = sparkSession.sparkContext.getConf.get("spark.app.name", SparkInfoTransfer.jobName)

    basePrepare(sparkSession, staticInputs, transforms, outputs)

    // when you see this ASCII logo, scistor compute platform is really started.
    showScistorAsciiLogo()

    if (staticInputs.nonEmpty) {
      registerInputTempViewWithHead(staticInputs, sparkSession)

      for (f <- transforms) {
        breakable {
          val df = viewTableMap.get(f.getConfig().getStepFrom)
          if(!df.isDefined) break
          val tempFrame = f.process(sparkSession, df.get)
          registerTempView(f.getConfig().getName, tempFrame)
        }
      }

      outputs.foreach(output => {
        sink(sparkSession, output, null)
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
                                  transforms: List[BaseTransform],
                                  outputs: List[BaseOutput]
                                 ): Unit = {
    val info = SparkInfoTransfer.jobInfo
    jobName = sparkSession.sparkContext.getConf.get("spark.app.name", SparkInfoTransfer.jobName)
    val duration: Long = info.getStepList.get(0).getStepInfo.getStepAttributes.get("properties").asInstanceOf[util.Map[String, AnyRef]].getOrDefault("batchDuration", "10").toString.toLong
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

          val tableName = streamingInput.getConfig().getName
          streamingInput.getConfig().getOutputFields.asScala.foreach(out => {
            ds = ds.withColumnRenamed(out.getStreamFieldName, out.getFieldName)
          })
          viewTableMap += (tableName -> ds)
          ds.persist(StorageLevel.MEMORY_AND_DISK)

          transforms.foreach(transform => {
            ds = transform.process(sparkSession, ds)
          })

          ds.unpersist()

          outputs.foreach(output => {
            sink(sparkSession, output, ds)
          })
        } else {
          logInfo(s"consumer 0 record!")
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
                                                     sparkSession: SparkSession): Unit = {
    if (staticInputs.nonEmpty) {
      for (input <- staticInputs.slice(0, staticInputs.length)) {
        var ds = input.getDataset(sparkSession)
        ds = convertDataType(ds, input)
        registerInputTempView(input, ds)
        // 写入mysql，统计输入量
        new JdbcUtil(sparkSession, SparkInfoTransfer.jobInfo.getMysqlConfig).writeDataCount("0", ds.count().toString, jobName)
      }
    } else {
      throw new RuntimeException("You must set static input plugin at least once.")
    }
  }

  private[scistor] def validate(plugins: List[Plugin]*): Unit = {
    var configValid = true
    for (pluginList <- plugins) {
      for (p <- pluginList) {
        val (isValid, msg) = Try(p.validate()) match {
          case Success(info) => {
            val (ret, message) = info
            (ret, message)
          }
          case Failure(exception) => (false, exception.getMessage)
        }

        if (!isValid) {
          configValid = false
          printf("[ERROR] Plugin[%s] contains invalid config, error: %s\n", p.name, msg)
        }
      }

      if (!configValid) {
        System.exit(-1) // invalid configuration
      }
    }
//    deployModeCheck()
  }

  private[scistor] def deployModeCheck(): Unit = {
    logInfo("preparing cluster mode work dir files...")

    // plugins.tar.gz is added in local app temp dir of driver and executors in cluster mode from --files specified in spark-submit
    val workDir = new File(".")
    logWarning("work dir exists: " + workDir.exists() + ", is dir: " + workDir.isDirectory)

    workDir.listFiles().foreach(f => logWarning("\t list file: " + f.getAbsolutePath))

    // decompress plugin dir
    val compressedFile = new File("plugins.tar.gz")

    Try(CompressionUtils.unGzip(compressedFile, workDir)) match {
      case Success(tempFile) => {
        Try(CompressionUtils.unTar(tempFile, workDir)) match {
          case Success(_) => logInfo("succeeded to decompress plugins.tar.gz")
          case Failure(ex) => {
            logError("failed to decompress plugins.tar.gz", ex)
            sys.exit(-1)
          }
        }

      }
      case Failure(ex) => {
        logError("failed to decompress plugins.tar.gz", ex)
        sys.exit(-1)
      }
    }
  }

  private[scistor] def showScistorAsciiLogo(): Unit = {
    AsciiArt.printAsciiArt("Scistor")
  }

  private[scistor] def showRuntimeError(throwable: Throwable): Unit = {
    println("\n\n==================================\n\n")
    println("Runtime Error, reason:\n")
    throwable.printStackTrace(System.out)
    println("\n====================================\n\n\n")
    throw new RuntimeException(throwable)
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
    val tableName = input.getConfig().getName
    registerTempView(tableName, ds)
  }

  private[scistor] def registerTempView(tableName: String, ds: Dataset[Row]): Unit = {
    ds.createOrReplaceTempView(tableName)
    viewTableMap += (tableName -> ds)
  }

  /**
   * Convert data source field type to spark schema dataType and drop unused columns
   */
  private[scistor] def convertDataType(ds: Dataset[Row], input: BaseStaticInput): DataFrame = {
    var df = ds
    // datasource schema dataType -> spark schema dataType
    input.getConfig().getOutputFields.foreach(field => {
      if (ds.columns.contains(field.getFieldName)) {
        val fieldType = field.getFieldType
        df = ds.withColumn(field.getFieldName, ds.col(field.getFieldName).cast(fieldType))
      }
    })
    // drop unused column
    val outputFields = input.getConfig().getOutputFields.map(_.getStreamFieldName)
    df.columns.foreach(col => {
      if (!outputFields.contains(col)) {
        df = df.drop(col)
      }
    })
    df
  }

  private[scistor] def sink(sparkSession: SparkSession,
                            output: BaseOutput,
                            df: DataFrame): Unit = {
    val config = output.getConfig()
    val option = viewTableMap.get(config.getStepFrom)
    breakable {
      var ds: Dataset[Row] = null
      if (df == null) {
        if (!option.isDefined) break
        ds = option.get
      } else {
        ds = df
      }

      config.getInputFields.foreach(out => {
        if (ds.columns.contains(out.getStreamFieldName)) {
          if (!out.getConstant) ds = ds.withColumn(out.getFieldName, ds.col(out.getStreamFieldName).cast(ComputeDataType.fromStructField(out.getFieldType)))
          else ds = ds.withColumn(out.getFieldName, new Column(out.getConstantValue))
        }
      })
      // 移除不输出的列
      val allCols = ds.schema.map(_.name)
      val needCols = config.getInputFields.map(_.getFieldName)
      allCols.foreach { col =>
        if (!needCols.contains(col)) ds = ds.drop(col)
      }

      println("[INFO] output dataframe: ")
      ds.show(5)

      // 写入mysql，统计输出
      new JdbcUtil(sparkSession, SparkInfoTransfer.jobInfo.getMysqlConfig).writeDataCount(ds.count().toString, "0", jobName)
      output.process(ds)
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
