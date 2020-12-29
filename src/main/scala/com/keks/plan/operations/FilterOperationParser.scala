package com.keks.plan.operations

import com.keks.plan.parser.ExpressionParser
import org.apache.spark.sql.catalyst.plans.logical.Filter


/**
  * Filter operation.
  * {{{
  *   .filter(col(NAME).isNotNull)
  * }}}
  */
case class FilterOperationParser(filter: Filter)(implicit parser: ExpressionParser) extends PlanOperation {

  override val operationName = FILTER

  val operationText = parser.toPrettyExpression(filter.condition, None, withoutTableName = true)

}
