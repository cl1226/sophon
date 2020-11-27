package com.scistor.operator.hive

import java.sql.{Connection, DriverManager}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import java.sql.PreparedStatement
import java.sql.ResultSet

object TestHive {

  def main(args: Array[String]): Unit = {

    val conf = new Configuration(true)
    conf.set("hadoop.security.authentication", "Kerberos")
    System.setProperty("keytab", "C:\\Users\\chenlou\\Desktop\\fsdownload\\user.keytab")
    System.setProperty("java.security.krb5.conf", "C:\\Users\\chenlou\\Desktop\\fsdownload\\krb5.conf")
    UserGroupInformation.setConfiguration(conf)
    UserGroupInformation.loginUserFromKeytab("scistor@HADOOP.COM", System.getProperty("keytab"))
    println("认证成功")

    var con: Connection = null
    var ps: PreparedStatement = null
    var rs: ResultSet = null
    try {
      Class.forName("org.apache.hive.jdbc.HiveDriver")
      con = DriverManager.getConnection("jdbc:hive2://scistor12:10000/default;principal=hive/_HOST@HADOOP.COM", null, null)
      ps = con.prepareStatement("show tables")
      rs = ps.executeQuery
      while (rs.next()) {
        System.out.println(rs.getString(1))
      }
    } catch {
      case e: Exception =>
        System.out.println("hive数据库连接失败\n" + e.getMessage)
    }

  }

}
