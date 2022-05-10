package com.keks.plan.operations.sources.utils

import com.keks.plan.operations.TransformationLogicTrait
import com.keks.plan.operations.sources.{HadoopSource, UnknownSource}
import org.apache.spark.sql.catalyst.AliasIdentifier
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.sources.BaseRelation

object DataSourceParserUtils {

  def parseSource(relation: LogicalRelation, p: Option[Project] = None, alias: Option[AliasIdentifier] = None): TransformationLogicTrait = {
    relation.relation match {
      case _: HadoopFsRelation => HadoopSource(relation, p, alias)
      case _ => UnknownSource(relation, p, alias)
    }
  }

  def getFileDatasourceName(relation: BaseRelation): SourceFilesInfo = {
    relation match {
      case HadoopFsRelation(location, _, _, _, fileFormat, _) =>
        val formatStr: String = fileFormat match {
          case jsonFileFormat: JsonFileFormat => jsonFileFormat.shortName
          case csvFileFormat: CSVFileFormat => csvFileFormat.shortName
          case parquetFileFormat: ParquetFileFormat => parquetFileFormat.shortName
        }
        SourceFilesInfo(formatStr, location.rootPaths.map(_.toString).toSet)

    }
  }

  def parseColumn(col: NamedExpression): SourceColumn = {
    col match {
      case alias: Alias =>
        SourceColumn(expId = col.exprId.id, name = col.name, dataType = alias.dataType)
      case AttributeReference(_, dataType, _, _) =>
        SourceColumn(expId = col.exprId.id, name = col.name, dataType = dataType)
    }
  }

}
