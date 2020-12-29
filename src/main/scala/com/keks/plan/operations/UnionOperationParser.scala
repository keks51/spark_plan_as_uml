package com.keks.plan.operations

import com.keks.plan.parser.ExpressionParser
import org.apache.spark.sql.catalyst.plans.logical.Union


/**
  * {{{
  *   .union(user)
  * }}}
  */
case class UnionOperationParser(union: Union)(implicit parser: ExpressionParser) extends PlanOperation {

  override val operationName = UNION

  override val operationText = ""

}
