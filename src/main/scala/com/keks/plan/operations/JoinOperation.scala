package com.keks.plan.operations

import org.apache.spark.sql.catalyst.plans.logical.Join


/**
  * Join operations.
  * {{{
  *   .join(rightDF, Seq(id), inner)
  * }}}
  */
case class JoinOperation(join: Join) extends PlanOperation {

  override val operationName = JOIN

  val operationText =
    join.joinType.sql.toUpperCase + "\n" +
      join.condition.map(toPrettyExpression(_, Some(join), withoutTableName = false)).getOrElse("UNKNOWN CONDITION")

}
