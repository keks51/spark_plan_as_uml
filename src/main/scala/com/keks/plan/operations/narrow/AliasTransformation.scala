package com.keks.plan.operations.narrow

import com.keks.plan.operations.{TABLE_ALIAS, TransformationLogicTrait}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.{Project, SubqueryAlias}
import org.apache.spark.sql.execution.datasources.LogicalRelation


/**
 * Reading source data operation or alias of existed data.
 * {{{
 *    .csv(...).as(USER)
 *    .join(...).filter(...).as(FILTERED_USER)
 *   }}}
  */
case class AliasTransformation(subqueryAlias: SubqueryAlias,
                               transformationText: String) extends TransformationLogicTrait {

  override val transformationName: String = TABLE_ALIAS

}

object AliasTransformation {

  def apply(subqueryAlias: SubqueryAlias): AliasTransformation = {
    val transformationText: String = subqueryAlias.child match {
      case _: Project =>
        "TableName: " + subqueryAlias.alias.toUpperCase + "\n"
      case _: UnresolvedRelation =>
        "TableName: " + subqueryAlias.alias.toUpperCase
      case relation: LogicalRelation =>
        "TableName: " + subqueryAlias.alias.toUpperCase
      case _ =>
        "TableName: " + subqueryAlias.alias.toUpperCase
    }
    AliasTransformation(subqueryAlias, transformationText)
  }

}
