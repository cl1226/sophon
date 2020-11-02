package com.scistor.compute

import java.io.File

import com.alibaba.fastjson.JSON
import com.scistor.compute.apis.{BaseOutput, BaseStaticInput, BaseStreamingInput, BaseTransform, Plugin}
import com.scistor.compute.model.remote.SparkTransDTO
import com.scistor.compute.model.spark.ProjectInfo
import com.scistor.compute.transform.UdfRegister
import com.scistor.compute.utils.CommonUtil.writeSimpleData
import com.scistor.compute.utils.{AsciiArt, CompressionUtils, ConfigBuilder, JdbcUtil, SparkInfoTransfer}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
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

    val str: String = "{\"transName\":\"模型_SQL脚本测试\",\"stepList\":[{\"stepFrom\":[],\"stepInfo\":{\"id\":\"123\",\"name\":\"test_in_b\",\"stepSource\":\"dataSource\",\"dataSourceStepType\":\"dataSourceInput\",\"stepType\":\"mysql\",\"stepId\":\"67ae9a3a140f49d98ee1a1d406fbb702\",\"stepAttributes\":{\"connectUrl\":\"jdbc:mysql://192.168.31.164:3306/test?characterEncoding=utf-8&serverTimezone=Asia/Shanghai\",\"dataBaseName\":\"test\",\"source\":\"source_1\",\"properties\":{\"password\":\"1122333\",\"user\":\"root\"},\"tableName\":\"source_1\"},\"inputFields\":[],\"outputFields\":[{\"fieldName\":\"sip\",\"streamFieldName\":\"sip\",\"fieldType\":\"string\",\"isConstant\":false,\"constantValue\":null},{\"fieldName\":\"sp\",\"streamFieldName\":\"sp\",\"fieldType\":\"string\",\"isConstant\":false,\"constantValue\":null},{\"fieldName\":\"dip\",\"streamFieldName\":\"dip\",\"fieldType\":\"string\",\"isConstant\":false,\"constantValue\":null},{\"fieldName\":\"dp\",\"streamFieldName\":\"dp\",\"fieldType\":\"string\",\"isConstant\":false,\"constantValue\":null},{\"fieldName\":\"src_c\",\"streamFieldName\":\"src_c\",\"fieldType\":\"string\",\"isConstant\":false,\"constantValue\":null},{\"fieldName\":\"dst_c\",\"streamFieldName\":\"dst_c\",\"fieldType\":\"string\",\"isConstant\":false,\"constantValue\":null},{\"fieldName\":\"ln1\",\"streamFieldName\":\"ln1\",\"fieldType\":\"string\",\"isConstant\":false,\"constantValue\":null}]}},{\"stepFrom\":[\"test_in_b\"],\"stepInfo\":{\"id\":\"456\",\"name\":\"bbb\",\"stepSource\":\"system\",\"dataSourceStepType\":\"\",\"stepType\":\"script\",\"stepId\":\"script\",\"stepAttributes\":{\"implementMethod\":\"sql-script\",\"codeBlock\":\"select * from test_in_b where sip<>'1'\"},\"inputFields\":[{\"fieldName\":\"a\",\"streamFieldName\":\"a\",\"fieldType\":\"string\",\"isConstant\":false,\"constantValue\":null}],\"outputFields\":[]}},{\"stepFrom\":[\"bbb\"],\"stepInfo\":{\"id\":\"789\",\"name\":\"bbbcopy\",\"stepSource\":\"system\",\"dataSourceStepType\":\"\",\"stepType\":\"script\",\"stepId\":\"script\",\"stepAttributes\":{\"implementMethod\":\"sql-script\",\"codeBlock\":\"select * from bbb where sip<>'168430086'\"},\"inputFields\":[{\"fieldName\":\"a\",\"streamFieldName\":\"a\",\"fieldType\":\"string\",\"isConstant\":false,\"constantValue\":null}],\"outputFields\":[{\"fieldName\":\"a\",\"streamFieldName\":\"a\",\"fieldType\":\"string\",\"isConstant\":false,\"constantValue\":null}]}},{\"stepFrom\":[\"bbbcopy\"],\"stepInfo\":{\"id\":\"abc\",\"name\":\"test_out_a\",\"stepSource\":\"dataSource\",\"dataSourceStepType\":\"dataSourceOutput\",\"stepType\":\"mysql\",\"stepId\":\"7e1a8000fe5046cd98c371c7543133b9\",\"stepAttributes\":{\"connectUrl\":\"jdbc:mysql://192.168.31.164:3306/test?characterEncoding=utf-8&serverTimezone=Asia/Shanghai\",\"dataBaseName\":\"test\",\"source\":\"sink_1\",\"properties\":{\"password\":\"1122333\",\"user\":\"root\"},\"tableName\":\"sink_1\"},\"inputFields\":[{\"fieldName\":\"sip\",\"streamFieldName\":\"sip\",\"fieldType\":\"string\",\"isConstant\":false,\"constantValue\":null},{\"fieldName\":\"sp\",\"streamFieldName\":\"sp\",\"fieldType\":\"string\",\"isConstant\":false,\"constantValue\":null},{\"fieldName\":\"dip\",\"streamFieldName\":\"dip\",\"fieldType\":\"string\",\"isConstant\":false,\"constantValue\":null},{\"fieldName\":\"dp\",\"streamFieldName\":\"dp\",\"fieldType\":\"string\",\"isConstant\":false,\"constantValue\":null},{\"fieldName\":\"src_c\",\"streamFieldName\":\"src_c\",\"fieldType\":\"string\",\"isConstant\":false,\"constantValue\":null},{\"fieldName\":\"dst_c\",\"streamFieldName\":\"dst_c\",\"fieldType\":\"string\",\"isConstant\":false,\"constantValue\":null},{\"fieldName\":\"ln1\",\"streamFieldName\":\"ln1\",\"fieldType\":\"string\",\"isConstant\":false,\"constantValue\":null}],\"outputFields\":[]}}],\"redisConfig\":{\"host\":\"192.168.31.219\",\"port\":6379,\"password\":null,\"database\":11},\"mysqlConfig\":{\"connection_url\":\"jdbc:mysql://192.168.31.164:3306/compute_product?characterEncoding=utf-8&serverTimezone=Asia/Shanghai&useSSL=false\",\"user_name\":\"root\",\"password\":\"1122333\",\"parameters\":null}}"

    val info = JSON.parseObject(str, classOf[SparkTransDTO])

    SparkInfoTransfer.transfer(info)

//    val jobApiDTO: JobApiDTO = JSON.parseObject(response.body, classOf[JobApiDTO])

//    val projectInfo = JobInfoTransfer.portalModel2SparkModel(jobApiDTO)

    val sparkConf = createSparkConf()
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    entrypoint()

  }

  private def entrypoint() = {
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
        val ds = input.getDataset(sparkSession)
        convertDataType(ds, input)
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
          printf("Plugin[%s] contains invalid config, error: %s\n", p.name, msg)
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
   * Convert data source field type to spark schema dataType
   */
  private[scistor] def convertDataType(ds: Dataset[Row], input: BaseStaticInput): Unit = {
    // 数据源schema dataType -> spark schema dataType
    input.getConfig().getOutputFields.foreach(field => {
      if (ds.columns.contains(field.getFieldName)) {
        val fieldType = field.getFieldType
        ds.withColumn(field.getFieldName, ds.col(field.getFieldName).cast(fieldType))
      }
    })
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

      config.getOutputFields.foreach(out => {
        if (ds.columns.contains(out.getFieldName)) {
          if (!out.getConstant) ds = ds.withColumn(out.getStreamFieldName, ds.col(out.getFieldName))
          else ds = ds.withColumn(out.getFieldName, new Column(out.getConstantValue))
        }
      })

      val allCols = ds.schema.map(_.name)
      val needCols = config.getOutputFields.asScala
      allCols.foreach { col =>
        if (!needCols.contains(col)) ds = ds.drop(col)
      }

      println("[INFO] output dataframe: ")
      ds.show(5)

      if (ds.take(1).size > 0) {
        writeSimpleData(ds, s"${SparkInfoTransfer.jobName} - ${config.getName}")
      }

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
