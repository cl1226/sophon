package com.scistor.compute.xian

import org.apache.spark.sql.SparkSession

object Runner {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("test").master("local[*]").getOrCreate()

    import session.implicits._

    session.sparkContext.setLogLevel("ERROR")
//    val url = "compute-xian/data/"
//    val rdd = session.sparkContext.binaryFiles(url)
//    val frame = rdd.map(x => (x._1, x._2.toArray())).toDF("ftpFileName", "ftpFileContent")
//    frame.show()

    val rdd = session.sparkContext.parallelize(Seq(("aaa", "bbb"), ("aaa", "bbb")))
    val frame = rdd.toDF("ftpFileName", "ftpFileContent")

    val demo = new Demo1
    val df = demo.transform(session, frame)

    df.printSchema()
    df.show()
  }

}
