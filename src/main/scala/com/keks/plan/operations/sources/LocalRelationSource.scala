package com.keks.plan.operations.sources

import com.keks.plan.operations.sources.utils.{DataSourceParserUtils, SourceColumn}
import com.keks.plan.operations.{CREATED_BY_CODE, TransformationLogicTrait}
import org.apache.spark.sql.catalyst.AliasIdentifier
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Project}


// TODO TEST it
case class LocalRelationSource(localRelation: LocalRelation, name: Option[AliasIdentifier] = None) extends TransformationLogicTrait {

  val sourceColumns: Array[SourceColumn] = localRelation.output
    .map(DataSourceParserUtils.parseColumn).toArray

  val transformationText: String =
    name.map(e => s"TableName: ${e.database.map(_ + ".").getOrElse("")}${e.identifier}\n").getOrElse("") +
      sourceColumns.map(_.printNameAndDataType).mkString("\n")

  override val transformationName: String = CREATED_BY_CODE

}
