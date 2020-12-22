package com.keks.plan.operations

import org.apache.spark.sql.catalyst.plans.logical.{Project, SubqueryAlias}


/**
  *  Reading source data operation or alias of existed data.
  *  {{{
  *    .csv(...).as(USER)
  *    .join(...).filter(...).as(FILTERED_USER)
  *  }}}
  */
case class SubqueryOperation(subqueryAlias: SubqueryAlias) extends PlanOperation {

  override val operationName = NAMED_SOURCE_TABLE

  val dataSource: Option[(String, Seq[String])] = detectDataSource(subqueryAlias.child)
  val dataSourceName = dataSource.map(e => s"${e._1}").getOrElse("ALIAS OF THE PREVIOUS TABLE")
  val operationText = subqueryAlias.child match {
    case project: Project =>
      "SourceType: " + dataSourceName.toUpperCase + "\n" +
        "TableName: " + subqueryAlias.alias.toUpperCase + "\n" +
        project.projectList.map(toPrettyExpression(_, None, dataSource.isDefined, withoutTableName = false).replaceAll(".*\\.", "")).mkString("\n")

  }
}