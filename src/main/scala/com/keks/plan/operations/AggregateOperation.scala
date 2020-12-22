package com.keks.plan.operations

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.Aggregate


/**
  * Processing Aggregate operation.
  * For example:
  * {{{
  *   .groupBy(ID, NAME)
      .agg(max(AGE))
  * }}}
  */
case class AggregateOperation(agg: Aggregate) extends PlanOperation {

  override val operationName = AGGREGATE
  override val operationText = {
    val groupExpr = agg
      .groupingExpressions.map(toPrettyExpression(_, None, withoutTableName = true))
      .groupedBy(5)
      .mkString(s",\n$INV3$INV3$INV3")
    val aggExpr = agg
      .aggregateExpressions
      .filterNot(_.isInstanceOf[AttributeReference])
      .map(toPrettyExpression(_, None, withoutTableName = true))
      .groupedBy(2)
      .mkString(s",\n$INV3$INV3$INV3$INV")
    s"""GROUP BY [$groupExpr]
       |$INV3 AGG [$aggExpr]
       |""".stripMargin
  }

}


