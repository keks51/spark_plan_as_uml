package com.keks.plan.operations.sources

import com.keks.plan.operations.sources.utils.{DataSourceParserUtils, SourceColumn}
import com.keks.plan.operations.{CREATED_BY_CODE, TransformationLogicTrait}
import org.apache.spark.sql.catalyst.AliasIdentifier
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.LogicalRDD


case class CreateDataframeSource(logicalRDD: LogicalRDD, name: Option[AliasIdentifier] = None) extends TransformationLogicTrait {

  val sourceColumns: Array[SourceColumn] = logicalRDD
    .output.map(DataSourceParserUtils.parseColumn).toArray

  val transformationText: String =
    name.map(e => s"TableName: ${e.database.map(_ + ".").getOrElse("")}${e.identifier}\n").getOrElse("") +
      sourceColumns.map(_.printNameAndDataType).mkString("\n")

  override val transformationName: String = CREATED_BY_CODE

}
