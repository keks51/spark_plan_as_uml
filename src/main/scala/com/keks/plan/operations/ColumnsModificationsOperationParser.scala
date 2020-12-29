package com.keks.plan.operations

import com.keks.plan.parser.ExpressionParser
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.Project


/**
  * Column modification operation.
  * For example:
  * {{{
  *   .withColumn(NAME_IS_NULL, when(col(NAME).isNull, true).otherwise(lit(false)))
  * }}}
  */
case class ColumnsModificationsOperationParser(project: Project)(implicit parser: ExpressionParser) extends PlanOperation {

  override val operationName = COLUMNS_MODIFICATIONS

  override val operationText = project
      .projectList
      .filterNot(_.isInstanceOf[AttributeReference])
      .map(parser.toPrettyExpression(_, None, withoutTableName = true))
      .mkString("\n")
}
