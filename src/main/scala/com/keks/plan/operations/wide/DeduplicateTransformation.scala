package com.keks.plan.operations.wide

import com.keks.plan.implicits.StringOps
import com.keks.plan.operations.{DEDUPLICATE, TransformationLogicTrait}
import org.apache.spark.sql.catalyst.plans.logical.Deduplicate

/**
 * Distinct or deduplicate operation.
 * For example:
 * {{{
 *   .distinct
 *   .dropDuplicates(...)
 * }}}
 */
case class DeduplicateTransformation(deduplicate: Deduplicate) extends TransformationLogicTrait {

  val columns: Seq[String] = deduplicate.keys.map(_.name)

  override val transformationName: String = DEDUPLICATE

  override val transformationText: String = columns.groupedBy(3).mkString("\n")

}
