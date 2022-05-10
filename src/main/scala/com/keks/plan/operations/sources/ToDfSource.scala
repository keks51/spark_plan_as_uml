package com.keks.plan.operations.sources

import com.keks.plan.operations.sources.utils.{DataSourceParserUtils, SourceColumn}
import com.keks.plan.operations.{CREATED_BY_CODE, TransformationLogicTrait}
import org.apache.spark.sql.catalyst.AliasIdentifier
import org.apache.spark.sql.catalyst.plans.logical.Project


case class ToDfSource(project: Project, name: Option[AliasIdentifier] = None) extends TransformationLogicTrait {

  val sourceColumns: Array[SourceColumn] = project
    .projectList.map(DataSourceParserUtils.parseColumn).toArray

  val transformationText: String =
    name.map(e => s"TableName: ${e.database.map(_ + ".").getOrElse("")}${e.identifier}\n").getOrElse("") +
      sourceColumns.map(_.printNameAndDataType).mkString("\n")

  val localRelationColumns: Array[SourceColumn] = project
    .child.output.map(DataSourceParserUtils.parseColumn).toArray

  override val transformationName: String = CREATED_BY_CODE

}
