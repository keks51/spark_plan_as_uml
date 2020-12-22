package com.keks.plan.operations

import org.apache.spark.sql.catalyst.plans.logical.Deduplicate


/**
  * Distinct or deduplicate operation.
  * For example:
  * {{{
  *   .distinct
  *   .dropDuplicates(...)
  * }}}
  */
case class DistinctOperation(deduplicate: Deduplicate) extends PlanOperation {

  val columns = deduplicate.keys.map(_.name)

  override val operationName = DEDUPLICATE

  override val operationText = columns.groupedBy(3).mkString("\n")

}
