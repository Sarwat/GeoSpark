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
import org.apache.spark.sql.sedona_sql.UDT.{GeometryUDT, RasterUDT}
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, BooleanType, DataType, DataTypes, DoubleType, IntegerType, LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.Geometry
import org.apache.spark.sql.sedona_sql.expressions.implicits._
import org.apache.spark.sql.sedona_sql.expressions.raster.implicits._
import org.geotools.coverage.grid.GridCoverage2D

import scala.reflect.runtime.universe.TypeTag
import scala.reflect.runtime.universe.Type
import scala.reflect.runtime.universe.typeOf

/**
 * This is the base class for wrapping Java/Scala functions as a catalyst expression in Spark SQL.
 * @param fSeq The functions to be wrapped. Subclasses can simply pass a function to this constructor,
 *          and the function will be converted to [[InferrableFunction]] by [[InferrableFunctionConverter]]
 *          automatically.
 */
abstract class InferredExpression(fSeq: InferrableFunction *)
  extends Expression with ImplicitCastInputTypes with SerdeAware with CodegenFallback with FoldableExpression
    with Serializable {

  def inputExpressions: Seq[Expression]

  lazy val f: InferrableFunction = fSeq match {
    // If there is only one function, simply use it and let org.apache.sedona.sql.UDF.Catalog handle default arguments.
    case Seq(f) => f
    // If there are multiple overloaded functions, find the one with the same number of arguments as the input
    // expressions. Please note that the Catalog won't be able to handle default arguments in this case. We'll
    // move default argument handling from Catalog to this class in the future.
    case _ => fSeq.find(f => f.sparkInputTypes.size == inputExpressions.size) match {
        case Some(f) => f
        case None => throw new IllegalArgumentException(s"No overloaded function ${getClass.getName} has ${inputExpressions.size} arguments")
      }
  }

  override def children: Seq[Expression] = inputExpressions
  override def toString: String = s" **${getClass.getName}**  "
  override def nullable: Boolean = true
  override def inputTypes: Seq[AbstractDataType] = f.sparkInputTypes
  override def dataType: DataType = f.sparkReturnType

  private lazy val argExtractors: Array[InternalRow => Any] = f.buildExtractors(inputExpressions)
  private lazy val evaluator: InternalRow => Any = f.evaluatorBuilder(argExtractors)

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
  implicit val gridCoverage2DInstance: InferrableType[GridCoverage2D] =
    new InferrableType[GridCoverage2D] {}
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
  implicit val intArrayInstance: InferrableType[Array[Int]] =
    new InferrableType[Array[Int]] {}
  implicit val javaIntArrayInstance: InferrableType[Array[java.lang.Integer]] =
    new InferrableType[Array[java.lang.Integer]]
  implicit val doubleArrayInstance: InferrableType[Array[Double]] =
    new InferrableType[Array[Double]] {}
  implicit val longInstance: InferrableType[Long] =
    new InferrableType[Long] {}
}

object InferredTypes {
  def buildArgumentExtractor(t: Type): Expression => InternalRow => Any = {
    if (t =:= typeOf[Geometry]) {
      expr => input => expr.toGeometry(input)
    } else if (t =:= typeOf[Array[Geometry]]) {
      expr => input => expr.toGeometryArray(input)
    } else if (t =:= typeOf[GridCoverage2D]) {
      expr => input => expr.toRaster(input)
    } else if (t =:= typeOf[Array[Double]]) {
      expr => input => expr.eval(input).asInstanceOf[ArrayData].toDoubleArray()
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
    } else if (t =:= typeOf[GridCoverage2D]) {
      output => {
        if (output != null) {
          output.asInstanceOf[GridCoverage2D].serialize
        } else {
          null
        }
      }
    } else if (t =:= typeOf[String]) {
      output =>
        if (output != null) {
          UTF8String.fromString(output.asInstanceOf[String])
        } else {
          null
        }
    } else if (t =:= typeOf[Array[java.lang.Long]] || t =:= typeOf[Array[Double]]) {
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
    } else if (t =:= typeOf[GridCoverage2D]) {
      RasterUDT
    } else if (t =:= typeOf[java.lang.Double]) {
      DoubleType
    } else if (t =:= typeOf[java.lang.Integer]) {
      IntegerType
    } else if (t =:= typeOf[Double]) {
      DoubleType
    } else if (t =:= typeOf[Int]) {
      IntegerType
    } else if (t =:= typeOf[Long]) {
      LongType
    } else if (t =:= typeOf[String]) {
      StringType
    } else if (t =:= typeOf[Array[Byte]]) {
      BinaryType
    } else if (t =:= typeOf[Array[Int]]) {
      DataTypes.createArrayType(IntegerType)
    } else if (t =:= typeOf[Array[java.lang.Integer]]) {
      DataTypes.createArrayType(IntegerType)
    } else if (t =:= typeOf[Array[java.lang.Long]]) {
      DataTypes.createArrayType(LongType)
    } else if (t =:= typeOf[Array[Double]]) {
      DataTypes.createArrayType(DoubleType)
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

}
