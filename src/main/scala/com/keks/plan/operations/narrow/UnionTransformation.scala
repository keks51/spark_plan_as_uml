package com.keks.plan.operations.narrow

import com.keks.plan.operations.{TransformationLogicTrait, UNION}
import com.keks.plan.parser.ExpressionParser
import org.apache.spark.sql.catalyst.plans.logical.Union

/**
 * {{{
 *   .union(user)
 * }}}
 */
case class UnionTransformation(union: Union)(implicit parser: ExpressionParser) extends TransformationLogicTrait {

  override val transformationName: String = UNION

  override val transformationText: String = ""

}
