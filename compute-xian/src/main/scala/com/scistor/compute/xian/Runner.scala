package com.scistor.compute.xian

import org.apache.spark.sql.SparkSession

object Runner {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("test").master("local[*]").getOrCreate()

    import session.implicits._

    session.sparkContext.setLogLevel("ERROR")
    val url = "ftp://ftpuser:123456@192.168.31.219:21/home/ftpuser/xian-0115"
    val rdd = session.sparkContext.binaryFiles(url)
    val frame = rdd.map(x => (x._1, x._2.toArray())).toDF("ftpFileName", "ftpFileContent")
    frame.show()

    val demo = new Demo1
    val df = demo.transform(session, frame)

    df.printSchema()
    df.show()
  }

}
