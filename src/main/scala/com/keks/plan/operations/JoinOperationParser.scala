package com.keks.plan.operations

import com.keks.plan.parser.ExpressionParser
import org.apache.spark.sql.catalyst.plans.logical.Join


/**
  * Join operations.
  * {{{
  *   .join(rightDF, Seq(id), inner)
  * }}}
  */
case class JoinOperationParser(join: Join)(implicit parser: ExpressionParser) extends PlanOperation {

  override val operationName = JOIN

  val operationText =
    join.joinType.sql.toUpperCase + "\n" +
      join.condition.map(parser.toPrettyExpression(_, Some(join), withoutTableName = false)).getOrElse("UNKNOWN CONDITION")

}
