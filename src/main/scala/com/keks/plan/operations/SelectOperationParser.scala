package com.keks.plan.operations

import com.keks.plan.parser.ExpressionParser
import org.apache.spark.sql.catalyst.plans.logical.Project


/**
  * Selecting columns.
  * {{{
  *   .select(a, b)
  * }}}
  */
case class SelectOperationParser(project: Project)(implicit parser: ExpressionParser) extends PlanOperation {

  override val operationName = SELECT

  override val operationText = project
    .projectList
    .map(parser.toPrettyExpression(_, None, withoutTableName = true))
    .mkString("\n")

}
