package com.keks.plan.operations.narrow

import com.keks.plan.operations.{FILTER, TransformationLogicTrait}
import com.keks.plan.parser.ExpressionParser
import org.apache.spark.sql.catalyst.plans.logical.Filter

/**
 * Filter operation.
 * {{{
 *   .filter(col(NAME).isNotNull)
 * }}}
 */
case class FilterTransformation(filter: Filter)(implicit parser: ExpressionParser) extends TransformationLogicTrait {

  override val transformationName: String = FILTER

  val transformationText: String = parser.toPrettyExpression(filter.condition, None, withoutTableName = true)

}
