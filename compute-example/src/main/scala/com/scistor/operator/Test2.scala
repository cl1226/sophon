package com.scistor.operator

object Test2 {

  def process(args: String*): String = {
    var res: String = "";
    args.foreach(x => {
      res += "," + x
    })
    res.substring(1)
  }

  def main(args: Array[String]): Unit = {
//    val res = process("a", "b")
//    println(res)
    val cmd = "source /opt"
    val cmd1 = s"'$cmd'"
    println(s"cmd1: $cmd1")
  }

}
