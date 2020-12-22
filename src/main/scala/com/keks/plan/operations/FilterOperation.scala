package com.keks.plan.operations

import org.apache.spark.sql.catalyst.plans.logical.Filter


/**
  * Filter operation.
  * {{{
  *   .filter(col(NAME).isNotNull)
  * }}}
  */
case class FilterOperation(filter: Filter) extends PlanOperation {

  override val operationName = FILTER

  val operationText = toPrettyExpression(filter.condition, None, withoutTableName = true)

}
