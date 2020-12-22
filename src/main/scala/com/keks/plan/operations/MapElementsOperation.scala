package com.keks.plan.operations

import org.apache.spark.sql.catalyst.plans.logical.MapElements


/**
  * Applying Class columns.
  * {{{
  *   .as[UserClass].map(identity)
  * }}}
  */
case class MapElementsOperation(mapElements: MapElements) extends PlanOperation {

  override val operationName = SCHEMA

  val schema = mapElements.argumentSchema.map(field => (field.name, field.dataType))

  override val operationText = schema
      .map(e => s"${e._1}: ${e._2.toString.toUpperCase.replace("TYPE", "")}")
      .mkString("\n")
      .replaceAll("\\(", "[")
      .replaceAll("\\)", "]")
      .replaceAll(" as ", " -> ")
      .replaceAll("STRUCTFIELD", "FIELD")

}
