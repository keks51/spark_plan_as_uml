package com.keks.plan.operations.wide

import com.keks.plan.implicits.StringOps
import com.keks.plan.operations.{AGGREGATE, TransformationLogicTrait}
import com.keks.plan.parser.ExpressionParser
import com.keks.plan.{INV, INV3}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.Aggregate

/**
 * Processing Aggregate operation.
 * For example:
 * {{{
 *   .groupBy(ID, NAME)
 *   .agg(max(AGE))
 * }}}
 */
case class AggregateTransformation(agg: Aggregate)(implicit parser: ExpressionParser) extends TransformationLogicTrait {

  override val transformationName: String = AGGREGATE
  override val transformationText: String = {
    val groupExpr = agg
      .groupingExpressions.map(parser.toPrettyExpression(_, None, withoutTableName = true))
      .groupedBy(5)
      .mkString(s",\n$INV3$INV3$INV3")
    val aggExpr = agg
      .aggregateExpressions
      .filterNot(_.isInstanceOf[AttributeReference])
      .map(parser.toPrettyExpression(_, None, withoutTableName = true))
      .groupedBy(2)
      .mkString(s",\n$INV3$INV3$INV3$INV")
    s"""GROUP BY [$groupExpr]
       |$INV3 AGG [$aggExpr]
       |""".stripMargin
  }

}
