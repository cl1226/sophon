package com.scistor.operator

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
    val str = process(Seq("aaa", "bbb"))
    println(str)
  }

}
