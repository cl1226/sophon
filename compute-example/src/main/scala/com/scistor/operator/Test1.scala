package com.scistor.operator

import java.text.{DateFormat, SimpleDateFormat}
import java.util.Calendar

object Test1 {

  def process(args: Seq[Any]): String = {
    var res: String = ""
    res = args(0) + "_" + args(1)

    import scalaj.http
    import scalaj.http.Http

    val response: http.HttpResponse[String] = Http("https://www.cnblogs.com/chhyan-dream/ajax/GetViewCount.aspx?postId=10732106").header("Accept", "application/json").timeout(10000, 1000).asString
    res + "_" + response.body
  }

  def main(args: Array[String]): Unit = {
//    val str = process(Seq("aaa", "bbb"))
//    println(str)
    var dateFormat: DateFormat = null
    val str: String = "yyyy-MM-dd/*"
    val hdfsTimeRange: Int = 1
    val calendar = Calendar.getInstance()
    val res = str.split("/").length match {
      case 2 => {
        val SP = "((.*)/(.*))".r
        val SP(all, a, b) = str.replace("*", "")
        dateFormat = new SimpleDateFormat(a)
        val now = dateFormat.format(calendar.getTime)
        b match {
          case "" => {
            calendar.add(Calendar.DATE, -hdfsTimeRange)
            val date = dateFormat.format(calendar.getTime)
            str.replace(a, date)
          }
          case _ => {
            calendar.add(Calendar.HOUR, -hdfsTimeRange)
            calendar.set(Calendar.MINUTE, 0)
            calendar.set(Calendar.SECOND, 0)
            calendar.set(Calendar.MILLISECOND, 0)
            val timestamp = calendar.getTime.getTime.toString
            str.replace(a, now).replace(b, timestamp)
          }
        }
      }
      case 3 => {
        val SP = "((.*)/(.*)/(.*))".r
        val SP(all, a, b, c) = str.replace("*", "")
        dateFormat = new SimpleDateFormat(a)
        val now = dateFormat.format(calendar.getTime)
        calendar.add(Calendar.HOUR, -hdfsTimeRange)
        calendar.set(Calendar.MINUTE, 0)
        calendar.set(Calendar.SECOND, 0)
        calendar.set(Calendar.MILLISECOND, 0)
        val timestamp = calendar.getTime.getTime.toString
        str.replace(a, now).replace(b, timestamp)
      }
    }
    println(res)
  }

}
