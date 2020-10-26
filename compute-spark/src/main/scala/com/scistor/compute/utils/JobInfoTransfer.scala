package com.scistor.compute.utils

import java.util

import com.scistor.compute.model.portal.{DataFormatType, DataSourceType, JobApiDTO, JobStepApiDTO, JobStepAttributeApiDTO}
import com.scistor.compute.model.spark._
import com.scistor.compute.redis.{JedisClient, JedisUtil}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.types.ComputeDataType

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.Breaks.{break, breakable}

object JobInfoTransfer {

  var jedis: JedisClient = _

  //global offline datasource need loaded
  val staticInputs = mutable.Map[String, SourceAttribute]()

  //global online datasource need loaded
  val streamingInputs = mutable.Map[String, SourceAttribute]()

  //global datasource need sink
  val outputs = mutable.Map[String, SinkAttribute]()

  def formatSinkAttr(step: JobStepApiDTO) = {
    val sink = new ComputeSinkAttribute()

    sink.bootstrap_urls = step.getDatabase.getBootstrapUrl
    sink.avroSchme = step.getDatabase.getAvroSchme
    sink.sink_connection_url = step.getDatabase.getConnectionUrl
    if (step.getDatabase.getDatabaseType == DataSourceType.Kafka) sink.sink_connection_url = step.getDatabase.getBootstrapUrl
    sink.csvSplit = step.getDatabase.getCsvSplit

    sink.sinkFormat = step.getDatabase.getDecodeType match {
      case DataFormatType.Avro => SinkFormat.AVRO
      case DataFormatType.Avro56 => SinkFormat.AVRO56
      case DataFormatType.Csv => SinkFormat.CSV
      case DataFormatType.Json => SinkFormat.JSON
      case DataFormatType.Parquet => SinkFormat.PARQUET
      case _ => null
    }

    sink.sinkType = step.getDatabase.getDatabaseType match {
      case DataSourceType.Kafka => SinkType.KAFKA
      case DataSourceType.Gbase => SinkType.GBASE
      case DataSourceType.MySQL => SinkType.MYSQL
      case DataSourceType.Oracle => SinkType.ORACLE
      case DataSourceType.Hive => SinkType.HIVE
      case DataSourceType.Hdfs => SinkType.HDFS
      case DataSourceType.Postgres => SinkType.POSTGRESQL
      case DataSourceType.Gaussdb => SinkType.GAUSSDB
      case _ => null
    }

    sink.sinkType match {
      case SinkType.MYSQL | SinkType.ORACLE | SinkType.GBASE | SinkType.PGSQL => sink.sink_process_class_fullname = "com.scistor.engine.sink.MyData2DbSink"
      case SinkType.KAFKA => sink.sink_process_class_fullname = "com.scistor.engine.sink.DataKafkaSink"
      case SinkType.ES => {
        sink.sink_connection_url = sink.sink_connection_url.split(",").transform(url=>{
          val nurl="http://"+url
          nurl
        }).mkString(",")
        sink.sink_process_class_fullname = "edp.wormhole.sinks.elasticsearchsink.Data2EsSink"
      }
      case _ =>
    }

    sink.groupid = step.getDatabase.getGroupId
    sink.isKerberos = step.getDatabase.isKerberos
    sink.sinknamespace = s"""${step.getDatabase.getDatabaseType}.${step.getDatabase.getDatabaseType}.${step.getDatabaseEnglish}.${step.getDatabase.getTableName}.*.*.*"""
    sink.topic = step.getDatabase.getTopic
    sink.sink_connection_username = step.getDatabase.getUsername
    sink.sink_connection_password = step.getDatabase.getPassword
    sink.tableName = step.getDatabase.getTableName
    sink.parameters = step.getDatabase.getParameters

    step.getInput.asScala.foreach(out => {
      out._2.asScala.foreach(i => {
        val streamDataType = ComputeDataType.fromStructFieldJson(i.getStreamFieldNameJson)
        val computeField = new ComputeField(streamDataType.name, DataType.STRING,true)
        computeField.setStructFieldJson(i.getStreamFieldNameJson)
        computeField.setConstant(i.isConstant)
        computeField.setConstantValue(i.getConstantValue)
        sink.getOutputMapping.put(i.getFieldName, computeField)
      })
    })

//    val structType = SSSparkSchmeUtils.parseStructType(step.getDatabase.getFieldJson)
//    sink.fields = parseSchemas(structType)
    sink.fieldsSparkJson = step.getDatabase.getFieldJson

    sink
  }

  def formatSourceAttr(step: JobStepApiDTO) = {
    val attribute = new ComputeAttribute

    attribute.bootstrap_urls = step.getDatabase.getBootstrapUrl
    attribute.avroSchme = step.getDatabase.getAvroSchme
    attribute.connection_url = step.getDatabase.getConnectionUrl
    attribute.csvSplit = step.getDatabase.getCsvSplit

    attribute.decodeType = step.getDatabase.getDecodeType match {
      case DataFormatType.Avro => DecodeType.AVRO
      case DataFormatType.Avro56 => DecodeType.AVRO56
      case DataFormatType.Csv => DecodeType.CSV
      case DataFormatType.Custom => DecodeType.USERDEFIND
      case DataFormatType.Json => DecodeType.JSON
      case DataFormatType.Parquet => DecodeType.PARQUET
      case _ => null
    }

    attribute.sourceType = step.getDatabase.getDatabaseType match {
      case DataSourceType.Kafka => SourceType.KAFKA
      case DataSourceType.Gbase => SourceType.GBASE
      case DataSourceType.MySQL => SourceType.MYSQL
      case DataSourceType.Oracle => SourceType.ORACLE
      case DataSourceType.Hive => SourceType.HIVE
      case DataSourceType.Hdfs => SourceType.HDFS
      case DataSourceType.Postgres => SourceType.POSTGRESQL
      case DataSourceType.Gaussdb => SourceType.GAUSSDB
      case _ => null
    }

    attribute.query = step.getDatabase.getQuery
    attribute.table_name = step.getDatabase.getTableName
    attribute.groupid = step.getDatabase.getGroupId
    attribute.isKerberos = step.getDatabase.isKerberos
    attribute.sourcenamespace = s"""${step.getDatabase.getDatabaseType}.${step.getDatabase.getDatabaseType}.${step.getDatabaseEnglish}.${step.getDatabase.getDatabaseEnglish}.*.*.*"""
    attribute.parameters = step.getDatabase.getParameters
    attribute.topic = step.getDatabase.getTopic
    attribute.isKerberos = step.getDatabase.isKerberos
    attribute.username = step.getDatabase.getUsername
    attribute.password = step.getDatabase.getPassword
    attribute.fieldsSparkJson = step.getDatabase.getFieldJson

//    val structType = org.apache.spark.sql.types.DataType.fromJson(step.getDatabase.getFieldJson)
//    attribute.fields = parseSchemas(structType)
    step.getOutput.asScala.foreach(out => {
      out._2.asScala.foreach(i => {
        val streamDataType = ComputeDataType.fromStructFieldJson(i.getStreamFieldNameJson)
        val computeField = new ComputeField(streamDataType.name, DataType.STRING,true)
        computeField.setStructFieldJson(i.getStreamFieldNameJson)
        computeField.setConstant(i.isConstant)
        computeField.setConstantValue(i.getConstantValue)
        attribute.getOutputMapping.put(i.getFieldName, computeField)
      })
    })

    attribute
  }

  def portalModel2SparkModel(job: JobApiDTO): ProjectInfo = {
    jedis = JedisUtil.getJedisClient(job.getRedisConfig)
    val info = new ProjectInfo
    info.setMysqlConfig(job.getMysqlConfig)
    info.setRedisConfig(job.getRedisConfig)
    info.setModuleName(job.getName)

    job.getStepList.asScala.foreach(step => {
      if (StringUtils.isNoneBlank(step.getPluginsType)) {
        val database = step.getDatabase
        if (StringUtils.isNoneBlank(step.getPluginsType) && step.getPluginsType.equals("input")) {
          val attribute = formatSourceAttr(step)
          info.setAttribute(attribute)
          database.getDatabaseType match{
            case DataSourceType.Kafka   =>{
              streamingInputs += (step.getName -> attribute)
            }
            case _ => staticInputs += (step.getName -> attribute)
          }
        } else if (StringUtils.isNoneBlank(step.getPluginsType) && step.getPluginsType.equals("output")) {
          val sink = formatSinkAttr(step)
          info.setSinkAttribute(sink)
          // TODO 不知道干嘛的？
//          step.getThinks.asScala.foreach(sinkName => {
//            outputs += (sinkName -> sink)
//          })
          outputs += (step.getName -> sink)
        }
      }
    })

    val buffer = job.getStepList.asScala.filter(step => StringUtils.isBlank(step.getPluginsType))
    breakable(
      buffer.foreach(step => {
        val myStep = step2ComputeJob(step, job)
        if ("CEP".equals(step.getImplementMethod)){
          val sink = new SinkAttribute
          sink.sinkType = SinkType.KAFKA
          sink.bootstrap_urls = info.getAttribute.bootstrap_urls
          sink.topic = s"""${myStep.getProjectName}_${myStep.getJobName}_cep"""
          sink.isKerberos = info.getAttribute.isKerberos
          sink.sink_connection_url = info.getAttribute.bootstrap_urls
          sink.sinkFormat = SinkFormat.JSON
          sink.sink_process_class_fullname = "com.scistor.engine.sink.DataKafkaSink"
          sink.sinknamespace = s"""kafka.cep.${myStep.getProjectName}.${myStep.getJobName}.*.*.**"""

          myStep.setSinkAttribute(sink)
          info.getRunJobs.add(myStep)
          break()
        }else{
          info.getRunJobs.add(myStep)
        }
      })
    )


    //cep 算子split
    for(i <- 0 until job.getStepList.size()){
      if ("CEP".equals(job.getStepList.get(i).getImplementMethod)) {
        val cep = step2ComputeJob(job.getStepList.get(i), job)

        val attribute = cep.getAttribute
        attribute.sourceType = SourceType.KAFKA
        attribute.sourcenamespace = s"""kafka.cep.${cep.getProjectName}.${cep.getJobName}.*.*.**"""
        attribute.decodeType = DecodeType.JSON
        attribute.bootstrap_urls = info.getAttribute.bootstrap_urls
        attribute.fields = info.getAttribute.fields
        attribute.groupid = "compute-spark"
        attribute.isKerberos = info.getAttribute.isKerberos
        attribute.topic = s"""${cep.getProjectName}_${cep.getJobName}"""

        val steps = new util.ArrayList[ComputeJob]()
        var j = i + 1
        while (j < job.getStepList.size()-1){
          if(!"CEP".equals(job.getStepList.get(j).getImplementMethod) && StringUtils.isBlank(job.getStepList.get(j).getPluginsType))
            steps.add(step2ComputeJob(job.getStepList.get(j), job))
          j = j + 1
        }
        //todo remove cep sink
        //        cepProcess += (cep -> steps)
      }
    }

    staticInputs.foreach(source => {
      packageIncrementInfo(info, source, jedis)
    })

    info
  }

  def step2ComputeJob(step: JobStepApiDTO, job: JobApiDTO): ComputeJob = {
    val myStep = new ComputeJob
    if (StringUtils.isBlank(step.getPluginsType)) {
      myStep.setProjectName(job.getName)
      myStep.setJobId(step.getPluginsId)
      myStep.setJobName(step.getName)
      myStep.setDataSource(step.getDatabaseEnglish)
      if (StringUtils.isNoneBlank(step.getPluginsId) && step.getPluginsId.equals("system-filter")) {
        myStep.setProcessSql(step.getQueryList)
      }
      if (StringUtils.isNoneBlank(step.getImplementMethod))
        myStep.setOperatorType(OperatorType.valueOf(step.getImplementMethod.replaceAll("-", "_")))

      step.getImplementMethod match {
        case "SQL" => {
          myStep.setProcessSql(step.getQueryList)
        }
        case "MAP" => {
          val input = step.getInput
          input.asScala.foreach(input => {
            val operator = new UserDefineOperator()
            operator.setClassFullName(step.getPluginsClass)
            operator.setMethodName(step.getPluginsFunctionName)
            input._2.asScala.foreach(mapping => {
              operator.getInputMapping.put(mapping.getStreamFieldName, parseFieldComputeField(mapping))
            })
            val output = step.getOutput.get(input._1)
            output.asScala.foreach(mapping => {
              operator.getOutputMapping.put(mapping.getFieldName, parseStreamComputeField(mapping))
            })
            myStep.getUdos.add(operator)
          })
        }

        case "PIPE" => {
          val fileName = step.getShellFile.substring(step.getShellFile.lastIndexOf("/")+1, step.getShellFile.length)
          val commands = new util.ArrayList[String]()
          if(fileName.endsWith("py")) commands.add("python") else if(fileName.endsWith("sh")) commands.add("sh")
          commands.add(fileName)
          val input = step.getInput
          input.asScala.foreach(input => {
            input._2.asScala.foreach(mapping => {
              myStep.getCommand.put(mapping.getStreamFieldName, commands)
              myStep.getCommandMaping.put(mapping.getStreamFieldName, step.getOutput.get(input._1).get(0).getStreamFieldName)
            })
          })

        }

        case "PRIVATE" =>

        case "UDF" =>

        case "JAVA" => {
          step.getInput.asScala.foreach(input => {
            val fields = new util.ArrayList[ComputeField]()
            input._2.asScala.foreach(attr => {
              val field = new ComputeField(attr.getStreamFieldName, DataType.STRING)
              field.setConstant(attr.isConstant)
              field.setConstantValue(attr.getConstantValue)
              field.setStructFieldJson(attr.getStreamFieldNameJson)
              fields.add(field)
            })
            val invokeInfo = new InvokeInfo(step.getPluginsClass, step.getPluginsFunctionName, fields)
            invokeInfo.setAlias(step.getOutput.get(input._1).get(0).getStreamFieldName)

            step.getOutput.get(input._1).asScala.map(x => {
              invokeInfo.getAliasField.add(List(x.getStreamFieldName, x.getFieldName).toArray)
            })

            invokeInfo.setCode(step.getCode)
            myStep.getProcess.add(invokeInfo)
          })
        }

        case "SCALA" => {
          step.getInput.asScala.foreach(input => {
            val fields = new util.ArrayList[ComputeField]()
            input._2.asScala.foreach(attr => {
              val field = new ComputeField(attr.getStreamFieldName, DataType.STRING)
              field.setConstant(attr.isConstant)
              field.setConstantValue(attr.getConstantValue)
              field.setStructFieldJson(attr.getStreamFieldNameJson)
              fields.add(field)
            })
            val invokeInfo = new InvokeInfo(step.getPluginsClass, step.getPluginsFunctionName, fields)
            invokeInfo.setAlias(step.getOutput.get(input._1).get(0).getStreamFieldName)
            invokeInfo.setCode(step.getCode)
            myStep.getProcess.add(invokeInfo)
          })
        }

        case "CEP" => {
          myStep.getFlinkSql.add(s"""cep = ${step.getCep}""")
        }

        case "siddhi_cep" => {
          myStep.setSiddhi_sql(step.getSiddhicep)
        }

        case _ =>

      }
      myStep.setAgg(step.isAgg)
      myStep.setAggCols(step.getAggCols)
    } else {
      //todo 数据源算子
      val table_name = step.getDatabaseEnglish
      val database = step.getDatabase
      if (StringUtils.isNoneBlank(step.getPluginsType) && step.getPluginsType.equals("input")) {
        val attribute = formatSourceAttr(step)
        if (database.getDatabaseType == DataSourceType.Kafka) myStep.setAttribute(attribute) else staticInputs += (table_name -> attribute)
      } else if (StringUtils.isNoneBlank(step.getPluginsType) && step.getPluginsType.equals("output")) {
        val sink = formatSinkAttr(step)
        myStep.setSinkAttribute(sink)
      }
    }
    myStep.getAttrs.putAll(step.getAttrs)
    myStep
  }

  def parseFieldComputeField(fielJson: JobStepAttributeApiDTO): ComputeField = {
    val structField2 = ComputeDataType.fromStructFieldJson(fielJson.getFieldNamejson)
    val field = new ComputeField(structField2.name, DataType.STRING, true)
    field.setStructFieldJson(fielJson.getFieldNamejson)
    field.setConstant(fielJson.isConstant)
    field.setConstantValue(fielJson.getConstantValue)
    field
  }

  def parseStreamComputeField(fielJson: JobStepAttributeApiDTO): ComputeField = {
    val structField2 = ComputeDataType.fromStructFieldJson(fielJson.getStreamFieldNameJson)
    val field = new ComputeField(structField2.name, DataType.STRING, true)
    field.setStructFieldJson(fielJson.getStreamFieldNameJson)
    field.setConstant(fielJson.isConstant)
    field.setConstantValue(fielJson.getConstantValue)
    field
  }

  /**
   * Increment data process
   * @param job
   * @param source
   * @param jedis
   */
  def packageIncrementInfo(job: ProjectInfo, source: (String, SourceAttribute), jedis: JedisClient): Unit ={
    var maxValue = "0"
    val column = source._2.parameters.getOrDefault("maxValueColumn", "")
    if(!"".equals(column)){
      if(jedis.exists(s"""${job.getProjectName}_${source._1}_maxValueColumn""")){
        maxValue = jedis.get(s"""${job.getProjectName}_${source._1}_maxValueColumn""")
      }

//      source._2.sourceType match {
//        case SourceType.MYSQL | SourceType.ORACLE | SourceType.GBASE | SourceType.PGSQL => {
//          var query = source._2.getQuery
//          val config = new ConnectConfig(source._2.getConnection_url, source._2.username, source._2.password)
//          val currentMax = new JdbcUtil(config).getMaxColumnValue(column, source._2.table_name)
//          jedis.set(s"""${job.getProjectName}_${source._1}_maxValueColumn""", currentMax)
//
//          val whereCase = {
//            if(!maxValue.equals("0"))
//              source._2.table_name + s" where $column > $maxValue and $column <= $currentMax"
//            else
//              source._2.table_name + s" where $column <= $currentMax"
//          }
//
//          query = query.replaceAll(source._2.table_name, whereCase)
//          source._2.setQuery(query)
//        }
//      }
    }
  }
}
