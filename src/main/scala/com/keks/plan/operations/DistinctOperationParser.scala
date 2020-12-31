package com.keks.plan.operations

import com.keks.plan.implicits.StringOps
import com.keks.plan.parser.ExpressionParser
import org.apache.spark.sql.catalyst.plans.logical.Deduplicate


/**
  * Distinct or deduplicate operation.
  * For example:
  * {{{
  *   .distinct
  *   .dropDuplicates(...)
  * }}}
  */
case class DistinctOperationParser(deduplicate: Deduplicate)(implicit parser: ExpressionParser) extends PlanOperation {

  val columns = deduplicate.keys.map(_.name)

  override val operationName = DEDUPLICATE

  override val operationText = columns.groupedBy(3).mkString("\n")

}
