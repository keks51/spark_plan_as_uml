package com.keks.plan.operations

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.Project


/**
  * Column modification operation.
  * For example:
  * {{{
  *   .withColumn(NAME_IS_NULL, when(col(NAME).isNull, true).otherwise(lit(false)))
  * }}}
  */
case class ColumnsModificationsOperation(project: Project) extends PlanOperation {

  override val operationName = COLUMNS_MODIFICATIONS

  override val operationText ={
    val res = project
      .projectList
      .filterNot(_.isInstanceOf[AttributeReference])
      .map(toPrettyExpression(_, None, withoutTableName = true))
      .mkString("\n")
    res
  }

}
