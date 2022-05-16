package com.keks.plan.operations.sources

import com.keks.plan.operations.TransformationLogicTrait
import com.keks.plan.operations.sources.utils.{DataSourceParserUtils, SourceColumn, SourceFilesInfo}
import org.apache.spark.sql.catalyst.AliasIdentifier
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.execution.datasources.LogicalRelation


case class HadoopSource(logical: LogicalRelation,
                        name: Option[AliasIdentifier] = None,
                        sourceFilesInfo: SourceFilesInfo,
                        sourceColumns: Array[SourceColumn],
                        transformationName: String,
                        transformationText: String) extends  TransformationLogicTrait {

  override def equals(obj: Any): Boolean = {
    obj.isInstanceOf[HadoopSource] &&
      obj.asInstanceOf[HadoopSource].sourceFilesInfo.equals(this.sourceFilesInfo)
  }
}

object HadoopSource {

  def apply(logical: LogicalRelation,
            p: Option[Project],
            name: Option[AliasIdentifier]): HadoopSource = {
    val sourceFilesInfo: SourceFilesInfo = DataSourceParserUtils.getFileDatasourceName(logical.relation)
    val sourceColumns: Array[SourceColumn] = p.map(_.output).getOrElse(logical.output)
      .map(e => DataSourceParserUtils.parseColumn(e)).toArray
    val transformationName: String = s"${sourceFilesInfo.name.toUpperCase}_FILE_SOURCE_TABLE"
    val transformationText: String =
      name.map(e => s"TableName: ${e.database.map(_ + ".").getOrElse("")}${e.identifier.toUpperCase}\n").getOrElse("") +
        sourceColumns.map(_.printNameAndDataType).mkString("\n")
    new HadoopSource(logical, name, sourceFilesInfo, sourceColumns, transformationName, transformationText)
  }

}