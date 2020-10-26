package org.apache.spark.sql.expression

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo}

class SSFunctionRegistry extends FunctionRegistry{
  override def registerFunction(name: FunctionIdentifier, info: ExpressionInfo, builder: FunctionBuilder): Unit = ???

  override def lookupFunction(name: FunctionIdentifier, children: Seq[Expression]): Expression = ???

  override def listFunction(): Seq[FunctionIdentifier] = ???

  override def lookupFunction(name: FunctionIdentifier): Option[ExpressionInfo] = ???

  override def lookupFunctionBuilder(name: FunctionIdentifier): Option[FunctionBuilder] = ???

  override def dropFunction(name: FunctionIdentifier): Boolean = ???

  override def clear(): Unit = ???
}
