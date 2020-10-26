package org.apache.spark.sql

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, ExpectsInputTypes, Expression, ExpressionDescription, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types
import org.apache.spark.sql.types.{BinaryType, BooleanType, CalendarIntervalType, DoubleType, FloatType, LongType, NullType, StringType, TimestampType, _}
import org.apache.spark.unsafe.types.UTF8String


class JavaReflect extends BinaryExpression{
  /**
    * Default behavior of evaluation according to the default nullability of BinaryExpression.
    * If subclass of BinaryExpression override nullable, probably should also override this.
    */
  override def eval(input: InternalRow): Any = {
    val value1 = left.eval(input)
    if (value1 == null) {
      null
    } else {
      val value2 = right.eval(input)
      if (value2 == null) {
        null
      } else {
        nullSafeEval(value1, value2)
      }
    }
  }

  /**
    * Called by default [[eval]] implementation.  If subclass of BinaryExpression keep the default
    * nullability, they can override this method to save null-check code.  If we need full control
    * of evaluation process, we should override [[eval]].
    */
  override def nullSafeEval(input1: Any, input2: Any): Any =
    sys.error(s"BinaryExpressions must override either eval or nullSafeEval")

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode ={
    ev
  }

  override def left: Expression = ???
  override def right: Expression = ???
  override def dataType: types.DataType = ???
  override def productElement(n: Int): Any = ???
  override def productArity: Int = ???
  override def canEqual(that: Any): Boolean = ???
}

/**
  * A function that get the absolute value of the numeric value.
  */
case class MyAbs(child: Expression)
  extends UnaryExpression with NullIntolerant {

  /**
    * Default behavior of evaluation according to the default nullability of UnaryExpression.
    * If subclass of UnaryExpression override nullable, probably should also override this.
    */
  override def eval(input: InternalRow): Any = {
    val value = child.eval(input)
    if (value == null) {
      null
    } else {
      nullSafeEval(value)
    }
  }

  override protected def doGenCode(
      ctx: CodegenContext,
      ev: ExprCode)
    : ExprCode = {
    nullSafeCodeGen(ctx, ev, (child) => {
      s"""${ev.value} = UTF8String.fromString(new org.scistor.example.Shixian().add1(($child).toString()));
       """})
  }

  override def dataType: DataType = IntegerType
}

abstract class ReflectExpression[E] extends Expression {
  /** Seed of the HashExpression. */
  val seed: E

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val hashResultType = ctx.javaType(dataType)
    dataType match {
      case NullType => ev
      case BooleanType => ???
      case ByteType | ShortType | IntegerType | DateType => ???
      case LongType => ???
      case TimestampType => ???
      case FloatType => ???
      case DoubleType => ???
      case d: DecimalType => ???
      case CalendarIntervalType => ???
      case BinaryType => ???
      case StringType => ???
      case ArrayType(et, containsNull) => ???
      case MapType(kt, vt, valueContainsNull) => ???
      case StructType(fields) => ???
      case udt: UserDefinedType[_] => ???
    }

  }

  def IntReflect(ev: ExprCode): Unit ={
    ev.copy(code = {
      s"""
         |${ev.value} = UTF8String.fromString(new org.scistor.example.Shixian().add1((code).toString()));
       """.stripMargin
    })
  }

  def StringReflect(ev: ExprCode): Unit ={
    ev.copy(code = {
      s"""
         |${ev.value} = UTF8String.fromString(new org.scistor.example.Shixian().add1((child).toString()));
       """.stripMargin
    })
  }


}

case class IntegerReflectExpression(child: Expression, className: String, method: String, isStatic: Boolean)
  extends UnaryExpression with NullIntolerant {

  /**
    * Default behavior of evaluation according to the default nullability of UnaryExpression.
    * If subclass of UnaryExpression override nullable, probably should also override this.
    */
  override def eval(input: InternalRow): Any = {
    val value = child.eval(input)
    if (value == null) {
      null
    } else {
      nullSafeEval(value)
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    var code = ""

    nullSafeCodeGen(ctx, ev, (child) => {
      if(isStatic) code = s"${className}.${method}($child)" else code = s"new ${className}().${method}($child)"
      s"""${ev.value} = ${code};
       """})
  }

  override def dataType: DataType = IntegerType
}


case class StringReflectExpression(child: Expression, className: String, method: String, isStatic: Boolean)
  extends UnaryExpression with NullIntolerant {

  /**
    * Default behavior of evaluation according to the default nullability of UnaryExpression.
    * If subclass of UnaryExpression override nullable, probably should also override this.
    */
  override def eval(input: InternalRow): Any = {
    val value = child.eval(input)
    if (value == null) {
      null
    } else {
      nullSafeEval(value)
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    var code = ""
    if(isStatic){
      code =
        s"""UTF8String.fromString(${className}.${method}(($child).toString()))
         """
      //        code =
      //          s"""
      //             |if(($child) instanceof org.apache.spark.unsafe.types.UTF8String){
      //             |  ${className}.${method}(($child)toString())
      //             |}else{
      //             |  ${className}.${method}($child)
      //             |}
      //           """.stripMargin
    } else {
      code =
        s"""UTF8String.fromString(new ${className}().${method}(($child).toString()))"""
      //        code =
      //          s"""
      //             |if(($child) instanceof org.apache.spark.unsafe.types.UTF8String){
      //             |  new ${className}().${method}(($child)toString())
      //             |}else{
      //             |  new ${className}().${method}($child)
      //             |}
      //           """.stripMargin
    }
    nullSafeCodeGen(ctx, ev, (child) => {
      s"""${ev.value} = ${code};
       """})
  }

  override def dataType: DataType = StringType
}