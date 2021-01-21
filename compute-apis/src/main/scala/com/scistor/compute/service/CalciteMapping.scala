package com.scistor.compute.service

import com.google.inject.Singleton
import com.scistor.compute.annotation.{HandlerMapping, Mappings}
import com.scistor.compute.http.MappingCtx
import com.scistor.compute.model.TestSchema
import io.netty.handler.codec.http.FullHttpRequest
import org.apache.calcite.adapter.java.ReflectiveSchema
import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.tools.Frameworks

@Singleton
@HandlerMapping(value = "/api/v1/calcite", httpMethod = "POST")
class CalciteMapping extends Mappings{
  override def run(req: FullHttpRequest, ctx: MappingCtx): Any = {
    val plus = Frameworks.createRootSchema(true)
    plus.add("T", new ReflectiveSchema(new TestSchema))
    val builder = Frameworks.newConfigBuilder()
    builder.defaultSchema(plus)
    val config = builder.build()
    val configBuilder = SqlParser.configBuilder(config.getParserConfig)
    configBuilder.setCaseSensitive(false).setConfig(configBuilder.build())
    val planner = Frameworks.getPlanner(config)
    val sqlNode = planner.parse("select \"a\".\"s\", count(\"a\".\"s\") from \"T\".\"rdf\" \"a\" group by \"a\".\"s\"")
    planner.validate(sqlNode)
    val relRoot = planner.rel(sqlNode)
    val node = relRoot.project()
    println(RelOptUtil.toString(node))
  }

}

object CalciteMapping {
  def main(args: Array[String]): Unit = {
    val mapping = new CalciteMapping
    mapping.run(null, null)
  }
}
