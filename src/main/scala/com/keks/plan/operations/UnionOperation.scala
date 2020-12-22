package com.keks.plan.operations

import org.apache.spark.sql.catalyst.plans.logical.Union


/**
  * {{{
  *   .union(user)
  * }}}
  */
case class UnionOperation(union: Union) extends PlanOperation {

  override val operationName = UNION

  override val operationText = ""

}
