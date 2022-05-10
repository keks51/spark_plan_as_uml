package com.keks.plan.operations.sources

import com.keks.plan.operations.TransformationLogicTrait
import com.keks.plan.operations.sources.utils.{DataSourceParserUtils, SourceColumn, SourceFilesInfo}
import org.apache.spark.sql.catalyst.AliasIdentifier
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.execution.datasources.LogicalRelation


case class UnknownSource(logical: LogicalRelation,
                         name: Option[AliasIdentifier] = None,
                         sourceFilesInfo: SourceFilesInfo,
                         sourceColumns: Array[SourceColumn],
                         transformationName: String,
                         transformationText: String
                        ) extends  TransformationLogicTrait {


  override def equals(obj: Any): Boolean = {
    obj.isInstanceOf[UnknownSource] &&
      obj.asInstanceOf[UnknownSource].sourceFilesInfo.equals(this.sourceFilesInfo)
  }
}

object UnknownSource {

  def apply(logical: LogicalRelation,
            p: Option[Project],
            name: Option[AliasIdentifier]): UnknownSource = {
    val sourceFilesInfo: SourceFilesInfo = DataSourceParserUtils.getFileDatasourceName(logical.relation)
    val sourceColumns: Array[SourceColumn] = p.map(_.output).getOrElse(logical.output)
      .map(e => DataSourceParserUtils.parseColumn(e)).toArray
    val transformationName: String = s"${sourceFilesInfo.name.toUpperCase}"
    val transformationText: String =
      name.map(e => s"TableName: ${e.database.map(_ + ".").getOrElse("")}${e.identifier}\n").getOrElse("") +
        sourceColumns.map(_.printNameAndDataType).mkString("\n")
        new UnknownSource(logical, name, sourceFilesInfo, sourceColumns, transformationName, transformationText)
  }

}