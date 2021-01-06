package com.scistor.compute

import com.google.inject.{Guice, Injector}
import com.scistor.compute.http.HttpServer
import com.scistor.compute.module.ComputeModule
import org.slf4j.LoggerFactory

object Application {

  private lazy val log = LoggerFactory.getLogger(this.getClass)

  val injector: Injector = Guice.createInjector(new ComputeModule)

  log.info("create dependencies injector")

  def main(args: Array[String]): Unit = {
    val server = injector.getInstance(classOf[HttpServer])
    server.start()
  }

}
