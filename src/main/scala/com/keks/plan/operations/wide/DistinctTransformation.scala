package com.keks.plan.operations.wide

import com.keks.plan.operations.{DISTINCT, TransformationLogicTrait}
import com.keks.plan.parser.ExpressionParser
import org.apache.spark.sql.catalyst.plans.logical.{Distinct, Project}

/**
 * Distinct  operation.
 * For example:
 * {{{
 *   .distinct
 *   .dropDuplicates(...)
 * }}}
 */
case class DistinctTransformation(distinct: Distinct)(implicit parser: ExpressionParser) extends TransformationLogicTrait {

  override val transformationName: String = DISTINCT

  override val transformationText: String = {
    distinct.child match {
      case project: Project =>
        project.projectList.map(parser.toPrettyExpression(_, None, withoutTableName = false).replaceAll(".*\\.", "")).mkString("\n")
      case _ =>
        "including all columns"
    }
  }

}
