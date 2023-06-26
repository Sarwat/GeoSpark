/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.spark.sql.sedona_sql.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, BooleanType, DataType, DataTypes, DoubleType, IntegerType, LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.Geometry
import org.apache.spark.sql.sedona_sql.expressions.implicits._

import scala.reflect.runtime.universe.TypeTag
import scala.reflect.runtime.universe.Type
import scala.reflect.runtime.universe.typeOf

/**
 * This is the base class for wrapping Java/Scala functions as a catalyst expression in Spark SQL.
 * @param f The function to be wrapped. Subclasses can simply pass a function to this constructor,
 *          and the function will be converted to [[InferrableFunction]] by [[InferrableFunctionConverter]]
 *          automatically.
 */
abstract class InferredExpression(f: InferrableFunction)
  extends Expression with ImplicitCastInputTypes with SerdeAware with CodegenFallback with FoldableExpression
    with Serializable {
  def inputExpressions: Seq[Expression]
  override def children: Seq[Expression] = inputExpressions
  override def toString: String = s" **${getClass.getName}**  "
  override def nullable: Boolean = true
  override def inputTypes: Seq[AbstractDataType] = f.sparkInputTypes
  override def dataType: DataType = f.sparkReturnType

  private val argExtractors: Array[InternalRow => Any] = f.buildExtractors(inputExpressions)
  private val evaluator: InternalRow => Any = f.evaluatorBuilder(argExtractors)

  override def eval(input: InternalRow): Any = f.serializer(evaluator(input))
  override def evalWithoutSerialization(input: InternalRow): Any = evaluator(input)
}

// This is a compile time type shield for the types we are able to infer. Anything
// other than these types will cause a compilation error. This is the Scala
// 2 way of making a union type.
sealed class InferrableType[T: TypeTag]
object InferrableType {
  implicit val geometryInstance: InferrableType[Geometry] =
    new InferrableType[Geometry] {}
  implicit val geometryArrayInstance: InferrableType[Array[Geometry]] =
    new InferrableType[Array[Geometry]] {}
  implicit val javaDoubleInstance: InferrableType[java.lang.Double] =
    new InferrableType[java.lang.Double] {}
  implicit val javaIntegerInstance: InferrableType[java.lang.Integer] =
    new InferrableType[java.lang.Integer] {}
  implicit val doubleInstance: InferrableType[Double] =
    new InferrableType[Double] {}
  implicit val booleanInstance: InferrableType[Boolean] =
    new InferrableType[Boolean] {}
  implicit val booleanOptInstance: InferrableType[Option[Boolean]] =
    new InferrableType[Option[Boolean]] {}
  implicit val intInstance: InferrableType[Int] =
    new InferrableType[Int] {}
  implicit val stringInstance: InferrableType[String] =
    new InferrableType[String] {}
  implicit val binaryInstance: InferrableType[Array[Byte]] =
    new InferrableType[Array[Byte]] {}
  implicit val longArrayInstance: InferrableType[Array[java.lang.Long]] =
    new InferrableType[Array[java.lang.Long]] {}
}

object InferredTypes {
  def buildArgumentExtractor(t: Type): Expression => InternalRow => Any = {
    if (t =:= typeOf[Geometry]) {
      expr => input => expr.toGeometry(input)
    } else if (t =:= typeOf[Array[Geometry]]) {
      expr => input => expr.toGeometryArray(input)
    } else if (t =:= typeOf[String]) {
      expr => input => expr.asString(input)
    } else {
      expr => input => expr.eval(input)
    }
  }

  def buildSerializer(t: Type): Any => Any = {
    if (t =:= typeOf[Geometry]) {
      output =>
        if (output != null) {
          output.asInstanceOf[Geometry].toGenericArrayData
        } else {
          null
        }
    } else if (t =:= typeOf[String]) {
      output =>
        if (output != null) {
          UTF8String.fromString(output.asInstanceOf[String])
        } else {
          null
        }
    } else if (t =:= typeOf[Array[java.lang.Long]]) {
      output =>
        if (output != null) {
          ArrayData.toArrayData(output)
        } else {
          null
        }
    } else if (t =:= typeOf[Array[Geometry]]) {
      output =>
        if (output != null) {
          ArrayData.toArrayData(output.asInstanceOf[Array[Geometry]].map(_.toGenericArrayData))
        } else {
          null
        }
    } else if (t =:= typeOf[Option[Boolean]]) {
      output =>
        if (output != null) {
          output.asInstanceOf[Option[Boolean]].orNull
        } else {
          null
        }
    } else {
      output => output
    }
  }

  def inferSparkType(t: Type): DataType = {
    if (t =:= typeOf[Geometry]) {
      GeometryUDT
    } else if (t =:= typeOf[Array[Geometry]]) {
      DataTypes.createArrayType(GeometryUDT)
    } else if (t =:= typeOf[java.lang.Double]) {
      DoubleType
    } else if (t =:= typeOf[java.lang.Integer]) {
      IntegerType
    } else if (t =:= typeOf[Double]) {
      DoubleType
    } else if (t =:= typeOf[Int]) {
      IntegerType
    } else if (t =:= typeOf[String]) {
      StringType
    } else if (t =:= typeOf[Array[Byte]]) {
      BinaryType
    } else if (t =:= typeOf[Array[java.lang.Long]]) {
      DataTypes.createArrayType(LongType)
    } else if (t =:= typeOf[Option[Boolean]]) {
      BooleanType
    } else {
      BooleanType
    }
  }
}

case class InferrableFunction(sparkInputTypes: Seq[AbstractDataType],
                              sparkReturnType: DataType,
                              serializer: Any => Any,
                              argExtractorBuilders: Seq[Expression => InternalRow => Any],
                              evaluatorBuilder: Array[InternalRow => Any] => InternalRow => Any) {
  def buildExtractors(expressions: Seq[Expression]): Array[InternalRow => Any] = {
    argExtractorBuilders.zipAll(expressions, null, null).flatMap {
      case (null, _) => None
      case (builder, expr) => Some(builder(expr))
    }.toArray
  }
}

object InferrableFunction {
  /**
   * Infer input types and return type from a type tag, and construct builder for argument extractors.
   * @param typeTag Type tag of the function.
   * @param evaluatorBuilder Builder for the evaluator.
   * @return InferrableFunction.
   */
  def apply(typeTag: TypeTag[_], evaluatorBuilder: Array[InternalRow => Any] => InternalRow => Any): InferrableFunction = {
    val argTypes = typeTag.tpe.typeArgs.init
    val returnType = typeTag.tpe.typeArgs.last
    val sparkInputTypes: Seq[AbstractDataType] = argTypes.map(InferredTypes.inferSparkType)
    val sparkReturnType: DataType = InferredTypes.inferSparkType(returnType)
    val serializer = InferredTypes.buildSerializer(returnType)
    val argExtractorBuilders = argTypes.map(InferredTypes.buildArgumentExtractor)
    InferrableFunction(sparkInputTypes, sparkReturnType, serializer, argExtractorBuilders, evaluatorBuilder)
  }

  /**
   * A variant of binary inferred expression which allows the second argument to be null.
   * @param f Function to be wrapped as a catalyst expression.
   * @param typeTag Type tag of the function.
   * @tparam R Return type of the function.
   * @tparam A1 Type of the first argument.
   * @tparam A2 Type of the second argument.
   * @return InferrableFunction.
   */
  def allowRightNull[R, A1, A2](f: (A1, A2) => R)(implicit typeTag: TypeTag[(A1, A2) => R]): InferrableFunction = {
    apply(typeTag, extractors => {
      val func = f.asInstanceOf[(Any, Any) => Any]
      val extractor1 = extractors(0)
      val extractor2 = extractors(1)
      input => {
        val arg1 = extractor1(input)
        val arg2 = extractor2(input)
        if (arg1 != null) {
          func(arg1, arg2)
        } else {
          null
        }
      }
    })
  }

  def allowSixRightNull[R, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13](f: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13) => R)
                                                                                  (implicit typeTag: TypeTag[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13) => R]): InferrableFunction = {
    apply(typeTag, extractors => {
      val func = f.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
      val extractor1 = extractors(0)
      val extractor2 = extractors(1)
      val extractor3 = extractors(2)
      val extractor4 = extractors(3)
      val extractor5 = extractors(4)
      val extractor6 = extractors(5)
      val extractor7 = extractors(6)
      val extractor8 = extractors(7)
      val extractor9 = extractors(8)
      val extractor10 = extractors(9)
      val extractor11 = extractors(10)
      val extractor12 = extractors(11)
      val extractor13 = extractors(12)
      input => {
        val arg1 = extractor1(input)
        val arg2 = extractor2(input)
        val arg3 = extractor3(input)
        val arg4 = extractor4(input)
        val arg5 = extractor5(input)
        val arg6 = extractor6(input)
        val arg7 = extractor7(input)
        val arg8 = extractor8(input)
        val arg9 = extractor9(input)
        val arg10 = extractor10(input)
        val arg11 = extractor11(input)
        val arg12 = extractor12(input)
        val arg13 = extractor13(input)
        if (arg1 != null && arg2 != null && arg3 != null && arg4 != null && arg5 != null && arg6 != null && arg7 != null) {
          func(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13)
        } else {
          null
        }
      }
    })
  }


  def allowTwoRightNull[R, A1, A2, A3, A4](f: (A1, A2, A3, A4) => R)(implicit typeTag: TypeTag[(A1, A2, A3, A4) => R]): InferrableFunction = {
    apply(typeTag, extractors => {
      val func = f.asInstanceOf[(Any, Any, Any, Any) => Any]
      val extractor1 = extractors(0)
      val extractor2 = extractors(1)
      val extractor3 = extractors(2)
      val extractor4 = extractors(3)
      input => {
        val arg1 = extractor1(input)
        val arg2 = extractor2(input)
        val arg3 = extractor3(input)
        val arg4 = extractor4(input)
        if (arg1 != null && arg2 != null && arg3 != null && arg4 != null) {
          func(arg1, arg2, arg3, arg4)
        } else {
          null
        }
      }
    })
  }

}
