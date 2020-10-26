package com.scistor.compute.utils

import java.lang.reflect.Method

import com.scistor.compute.model.spark.UserDefineOperator

object ClassUtils extends Serializable {
  def newClassInstance(classfullname: String): Any = {
    Class.forName(classfullname).newInstance()
  }

  def getUserOperatorImpl(udo: UserDefineOperator): Any = {
    val bean = Class.forName(udo.getClassFullName.trim).newInstance()
    bean
  }

  def loadInstacneAndMethod(classFullName: String, methodName: String): (Any, Method) ={
    val intacne = {
      try {
        val clazz = Class.forName(classFullName, true, Thread.currentThread().getContextClassLoader)
        clazz.newInstance()
      } catch {
        case e: ClassNotFoundException => e.printStackTrace()
      }
    }
    val method = intacne.getClass.getMethods.find { method =>
      val candidateTypes = method.getParameterTypes
      if (method.getName != methodName) {
        // Name must match
        false
      }else {
        true
      }
    }.get
    (intacne, method)
  }
}
