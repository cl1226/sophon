package com.scistor.compute.common

import com.google.inject.{Injector, Singleton}
import com.scistor.compute.Application
import com.scistor.compute.annotation.{HandlerMapping, Mappings}
import com.scistor.compute.http.MappingCase
import org.reflections.Reflections
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.jdk.CollectionConverters
import scala.util.matching.Regex

@Singleton
class MappingScanner {

  private val log = LoggerFactory.getLogger(this.getClass)

  val injector: Injector = Application.injector

  def routerMapping(packageName: String = "*"): mutable.Set[MappingCase] = {
    val reflections: Reflections = new Reflections(packageName)
    log.info("scan mappings,package:{}", packageName)
    CollectionConverters.SetHasAsScala(reflections.getTypesAnnotatedWith(classOf[HandlerMapping]))
      .asScala
      .map(k => {
        val mapping = k.getAnnotation(classOf[HandlerMapping])
        MappingCase(mapping.httpMethod(), injector.getInstance(k).asInstanceOf[Mappings], methodMatcher(mapping))
      })
  }

  private def methodMatcher(mapping: HandlerMapping): Regex = ("^" + mapping.value() + "$").r
}
