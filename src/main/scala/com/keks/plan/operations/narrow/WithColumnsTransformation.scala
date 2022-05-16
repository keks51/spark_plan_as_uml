package com.keks.plan.operations.narrow

import com.keks.plan.operations.{COLUMNS_MODIFICATIONS, TransformationColumn, TransformationLogicTrait}
import com.keks.plan.parser.ExpressionParser
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.Project

case class WithColumnsTransformation(project: Project)(implicit parser: ExpressionParser) extends TransformationLogicTrait {

  override val transformationName: String = COLUMNS_MODIFICATIONS

  val columns: Array[TransformationColumn] = project
    .projectList
    .filterNot(_.isInstanceOf[AttributeReference])
    .map(namedExpression =>
      TransformationColumn(namedExpression.exprId.id, parser.toPrettyExpression(namedExpression, None, withoutTableName = true))
    )
    .toArray

  override val transformationText: String = columns.map(_.name).mkString("\n")

}
