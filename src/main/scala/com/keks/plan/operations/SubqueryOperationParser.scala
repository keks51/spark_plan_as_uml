package com.keks.plan.operations

import com.keks.plan.parser.ExpressionParser
import org.apache.spark.sql.catalyst.plans.logical.{Project, SubqueryAlias}


/**
  *  Reading source data operation or alias of existed data.
  *  {{{
  *    .csv(...).as(USER)
  *    .join(...).filter(...).as(FILTERED_USER)
  *  }}}
  */
case class SubqueryOperationParser(subqueryAlias: SubqueryAlias)(implicit parser: ExpressionParser) extends PlanOperation {

  override val operationName = NAMED_SOURCE_TABLE

  val dataSource: Option[(String, Seq[String])] = parser.detectDataSource(subqueryAlias.child)
  val dataSourceName = dataSource.map(e => s"${e._1}").getOrElse("ALIAS OF THE PREVIOUS TABLE")
  val operationText = subqueryAlias.child match {
    case project: Project =>
      "SourceType: " + dataSourceName.toUpperCase + "\n" +
        "TableName: " + subqueryAlias.alias.toUpperCase + "\n" +
        project.projectList.map(parser.toPrettyExpression(_, None, dataSource.isDefined, withoutTableName = false).replaceAll(".*\\.", "")).mkString("\n")

  }
}
