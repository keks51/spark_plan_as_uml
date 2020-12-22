package com.keks.plan.operations

import org.apache.spark.sql.catalyst.plans.logical.Window


/**
  * Window operation.
  * {{{
  *   .withColumn(RANK, row_number.over(partitionBy(ID).orderBy(asc(CALENDAR_DATE))))
  * }}}
  */
case class WindowOperation(window: Window) extends PlanOperation {

  override val operationName = WINDOW

  override val operationText = window
    .windowExpressions
    .map(toPrettyExpression(_, Some(window), withoutTableName = true))
    .mkString("\n")

}
