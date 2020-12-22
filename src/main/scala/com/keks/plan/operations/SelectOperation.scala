package com.keks.plan.operations

import org.apache.spark.sql.catalyst.plans.logical.Project


/**
  * Selecting columns.
  * {{{
  *   .select(a, b)
  * }}}
  */
case class SelectOperation(project: Project) extends PlanOperation {

  override val operationName = SELECT

  override val operationText = project
    .projectList
    .map(toPrettyExpression(_, None, withoutTableName = true))
    .mkString("\n")

}
