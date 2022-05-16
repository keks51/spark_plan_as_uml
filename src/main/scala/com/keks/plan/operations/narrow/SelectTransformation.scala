package com.keks.plan.operations.narrow

import com.keks.plan.operations.{SELECT, TransformationLogicTrait}
import com.keks.plan.parser.ExpressionParser
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.plans.logical.Project

/**
 * Selecting columns.
 * {{{
 *   .select(a, b)
 * }}}
 */
case class SelectTransformation(project: Project)(implicit parser: ExpressionParser) extends TransformationLogicTrait {

  val columns: Seq[NamedExpression] = project.projectList

  override val transformationName: String = SELECT

  override val transformationText: String = project
    .projectList
    .map(parser.toPrettyExpression(_, None, withoutTableName = true))
    .mkString("\n")

}
