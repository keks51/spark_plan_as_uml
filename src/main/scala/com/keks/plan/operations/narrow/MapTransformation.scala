package com.keks.plan.operations.narrow

import com.keks.plan.Utils.stringClassNameToTypeTag
import com.keks.plan.operations.{MAP_TRANSFORMATION, TransformationLogicTrait}
import com.keks.plan.parser.ExpressionParser
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.plans.logical.MapElements
import org.apache.spark.sql.types.{DataType, ObjectType}

import scala.reflect.runtime.universe
import scala.util.Try

/**
 * Applying Class columns.
 * {{{
 *   .as[UserClass].map(identity)
 * }}}
 */
case class MapTransformation(mapElements: MapElements)(implicit parser: ExpressionParser) extends TransformationLogicTrait {

  override val transformationName: String = MAP_TRANSFORMATION

  val schema: Seq[(String, DataType)] = mapElements.outputObjAttr.dataType match {
    case obj: ObjectType =>
      val typeTag: universe.TypeTag[Product] = stringClassNameToTypeTag(obj.simpleString)
      Try(Encoders.product(typeTag).schema.map(field => (field.name, field.dataType)))
        .toOption
        .getOrElse(Seq(("", obj)))
    case e => Seq(("", e))
  }


  override val transformationText: String = schema
    .map(e => s"${e._1}: ${e._2}")
    .mkString("\n")
    .replaceAll("\\(", "[")
    .replaceAll("\\)", "]")
    .replaceAll(" as ", " -> ")
    .replaceAll("STRUCTFIELD", "FIELD")

}
