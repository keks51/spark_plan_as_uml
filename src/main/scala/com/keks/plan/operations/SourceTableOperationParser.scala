package com.keks.plan.operations

import com.keks.plan.parser.ExpressionParser
import org.apache.spark.sql.catalyst.plans.logical.Project


/**
  * Reading source data operation.
  * {{{
  *   .csv(...)
  * }}}
  */
case class SourceTableOperationParser(project: Project)(implicit parser: ExpressionParser) extends PlanOperation {

  override val operationName = SOURCE_TABLE

  val dataSource: Option[(String, Seq[String])] = parser.detectDataSource(project.child)
  val dataSourceName = dataSource.map(e => s"${e._1}\n").getOrElse("")
  val operationText =
      "SourceType: " + dataSourceName.toUpperCase + "\n" +
        project.projectList.map(parser.toPrettyExpression(_, None, dataSource.isDefined, withoutTableName = true)).mkString("\n")

}
