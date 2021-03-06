# sophon
### 产品介绍
本产品是一款易用、高性能、支持实时流式和离线批处理的海量数据处理产品。平台架构于Apache Spark框架之上，
提供可视化界面以拖拉拽方式迅速完成数据流模型的建立，加快分布式数据处理能力在生产环境落地。
### 运行方式
配合平台提供的界面操作快速的生成业务模型，支持在页面上配置任务运行所需资源、yarn队列等信息。组装spark-submit命令，
通过远程发起shell命令，提交任务至Yarn集群。平台支持监控任务的运行状态，获取任务执行日志等信息。
### 产品结构
```mermaid
[HTML] -> [Micro Service] -> [Spark]
```

### 使用场景
1. 海量数据ETL
2. 海量数据聚合
3. 多源数据处理
### 算子配置
> 通过可视化界面完成代码的编写, Jar包的上传, SQL的编写, 脚本的编写
1. 支持Java code/Scala code
2. 支持Java Jar/Scala Jar
3. 支持SQL
4. 支持Python/Shell脚本
### 支持的数据源
> 支持对多种数据源的读写，支持Kerberos
1. Hdfs(支持配置不同目录结构)
    - yyyy-MM-dd/timestamp*
    - yyyyMMdd/timestamp*
    - yyyy-MM-dd/timestamp/*
    - yyyyMMdd/timestamp/*
    - yyyy-MM-dd/*
    - yyyyMMdd/*
2. Hive
3. Mysql/Oracle/Postgresql/Gaussdb/Gbase
   - 读取
     - 全量读取
     - 增量字段读取
     - 条件读取
     - 并发读取，通过设置numPartitions,[partColumnName]
   - 写入
     - jdbc(使用sparksession api)
     - insert(使用数据库连接池, 拼SQL)
     - copy(针对Postgresql/Gaussdb)
4. Elasticsearch
5. File(json/csv/txt)
6. Kafka
   - kerberos认证
     - security.protocol=SASL_PLAINTEXT
     - sasl.mechanism=GSSAPI
     - sasl.kerberos.service.name=kafka
     - java.security.auth.login.config=./sparkkafkajaas.conf
     - java.security.krb5.conf=./krb5.conf
7. Ftp(json/csv/txt)
8. Http(json)
### 支持内置算子
> 支持在页面上配置相应内置算子所必须的参数，完成数据流的转换
1. Join(sourceTableName, targetTableName, joinField[List], joinType[default: inner])
2. Repartition(numPartitions)
3. Sample(fraction, limit)
4. Checksum(method[CRC32, MD5, SHA1], sourceField, targetField)
5. Convert(sourceField, newType)
6. Constant(constantMap[Map[String, string]])
7. Script(JavaCode/ScalaCode/Sql/Python/Shell)
8. UserDefinedTransform(JavaJar/ScalaJar/JavaCode/ScalaCode/Sql/Python/Shell)