package org.apache.spark.sql.expression

import com.scistor.compute.until.CompileUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, LeafExpression}
import org.apache.spark.sql.types.{AbstractDataType, DataType, DataTypes}
import org.apache.spark.unsafe.types.UTF8String

case class CodeGenExpress(children: Seq[Expression]) extends Expression with ExpectsInputTypes with CodegenFallback {

  override def inputTypes: Seq[AbstractDataType] = Seq(DataTypes.StringType, DataTypes.StringType, DataTypes.StringType)

  private lazy val genCode = children(0).eval().asInstanceOf[UTF8String].toString

  private lazy val method_name = children(1).eval().asInstanceOf[UTF8String].toString

  private lazy val param = children(2).eval().asInstanceOf[UTF8String].toString


  @transient private lazy val method = {
    CompileUtils.getMethodByCode("Eval"+method_name, genCode, method_name)
  }

//  @transient private lazy val return_name: String = method.getReturnType.getName
//  @transient private lazy val atomicType: AtomicType = UdfUtils.convertSparkType(return_name)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    ctx.addNewFunction(method_name, genCode, true)
    // LeafNode does not need `input`
    val input = if (this.isInstanceOf[LeafExpression]) "null" else ctx.INPUT_ROW
    //${genCode}
    val code = s"""
        ${ctx.javaType(dataType)} ${ev.value} = $method_name(${input}.get(param));
      """
//    val code = s"""
//        ${genCode}
//        ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
//      """
    ev.copy(code)
  }

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = input

  override def dataType: DataType = DataTypes.StringType
}
