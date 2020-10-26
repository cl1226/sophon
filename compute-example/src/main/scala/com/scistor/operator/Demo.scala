package com.scistor.operator

import java.io.File
import java.net.{URL, URLClassLoader}

import scala.io.Source
import scala.reflect.runtime.universe
import scalaj.http
import scalaj.http.Http

import scala.tools.reflect.ToolBox

object Demo {

  private val classLoader = scala.reflect.runtime.universe.getClass.getClassLoader

  classLoader.loadClass("scalaj.http.Http")

  private val toolBox = universe.runtimeMirror(classLoader).mkToolBox()

  def main(args: Array[String]): Unit = {
    val demo = new Demo
    demo func "xxx"

    val file = "C:\\Users\\Mary\\Desktop\\Test1.scala"
    val src = Source.fromFile(file).mkString.stripMargin

    val zz = toolBox.compile(toolBox.parse(src)).apply().asInstanceOf[Class[_]]
    val method = zz.getDeclaredMethods.head

    val res = method.invoke(zz.newInstance(), Seq("aa", "bb"))
    println(res)
  }

}
class Demo {
  def func(a: String): Unit = {
    println(a)
  }
}
