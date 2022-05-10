package com.keks.plan.operations.wide

import com.keks.plan.operations.{TransformationLogicTrait, WINDOW}
import com.keks.plan.parser.ExpressionParser
import org.apache.spark.sql.catalyst.plans.logical.Window


/**
 * Window operation.
 * {{{
 *   .withColumn(RANK, row_number.over(partitionBy(ID).orderBy(asc(CALENDAR_DATE))))
 * }}}
 */
case class WindowTransformation(window: Window)(implicit parser: ExpressionParser) extends TransformationLogicTrait {

  override val transformationName: String = WINDOW

  override val transformationText: String = window
    .windowExpressions
    .map(parser.toPrettyExpression(_, Some(window), withoutTableName = true))
    .mkString("\n")

}
