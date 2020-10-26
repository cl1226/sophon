package com.scistor.compute.until

import java.lang.reflect.Method
import java.util
import java.util.UUID

import scala.reflect.runtime.universe
import scala.tools.reflect.ToolBox

case class ClassInfo(clazz: Class[_], instance: Any, defaultMethod: Method, methods: Map[String, Method]) {
  def invoke[T](args: Object*): T = {
    defaultMethod.invoke(instance, args: _*).asInstanceOf[T]
  }
}

object ClassCreateUtils {
  private val clazzs = new util.HashMap[String, ClassInfo]()
  private val classLoader = scala.reflect.runtime.universe.getClass.getClassLoader
  private val toolBox = universe.runtimeMirror(classLoader).mkToolBox()


  def apply(func: String): ClassInfo = this.synchronized {
    var clazz = clazzs.get(func)
    if (clazz == null) {
      val (className, classBody) = wrapClass(func)
      val zz = compile(prepareScala(className, classBody))
      val defaultMethod = zz.getDeclaredMethods.head
      val methods = zz.getDeclaredMethods
      clazz = ClassInfo(
        zz,
        zz.newInstance(),
        defaultMethod,
        methods = methods.map { m => (m.getName, m) }.toMap
      )
      clazzs.put(func, clazz)
    }
    clazz
  }

  def compile(src: String): Class[_] = {
    val tree = toolBox.parse(src)
    toolBox.compile(tree).apply().asInstanceOf[Class[_]]
  }

  def prepareScala(className: String, classBody: String): String = {
    classBody + "\n" + s"scala.reflect.classTag[$className].runtimeClass"
  }

  def wrapClass(function: String): (String, String) = {
    val className = s"dynamic_class_${UUID.randomUUID().toString.replaceAll("-", "")}"
    val classBody =
      s"""
         |import scalaj.http
         |import scalaj.http.Http
         |
         |class $className extends Serializable{
         |  $function
         |}
            """.stripMargin
    (className, classBody)
  }

}
