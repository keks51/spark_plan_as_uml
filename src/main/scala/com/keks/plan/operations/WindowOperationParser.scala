package com.keks.plan.operations

import com.keks.plan.parser.ExpressionParser
import org.apache.spark.sql.catalyst.plans.logical.Window


/**
  * Window operation.
  * {{{
  *   .withColumn(RANK, row_number.over(partitionBy(ID).orderBy(asc(CALENDAR_DATE))))
  * }}}
  */
case class WindowOperationParser(window: Window)(implicit parser: ExpressionParser) extends PlanOperation {

  override val operationName = WINDOW

  override val operationText = window
    .windowExpressions
    .map(parser.toPrettyExpression(_, Some(window), withoutTableName = true))
    .mkString("\n")

}
