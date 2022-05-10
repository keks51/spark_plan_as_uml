package com.keks.plan.operations.wide

import com.keks.plan.operations.{JOIN, TransformationLogicTrait}
import com.keks.plan.parser.ExpressionParser
import org.apache.spark.sql.catalyst.plans.logical.Join

/**
 * Join operations.
 * {{{
 *   .join(rightDF, Seq(id), inner)
 * }}}
 */
case class JoinTransformation(join: Join)(implicit parser: ExpressionParser) extends TransformationLogicTrait {

  override val transformationName: String = JOIN

  val transformationText: String =
    join.joinType.sql.toUpperCase + "\n" +
      join.condition.map(parser.toPrettyExpression(_, Some(join), withoutTableName = false)).getOrElse("UNKNOWN CONDITION")

}
