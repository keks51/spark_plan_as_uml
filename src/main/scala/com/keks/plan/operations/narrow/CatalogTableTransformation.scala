package com.keks.plan.operations.narrow

import com.keks.plan.operations.{CATALOG_SOURCE_TABLE, TransformationLogicTrait}
import com.keks.plan.parser.ExpressionParser
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation

case class CatalogTableTransformation(relation: UnresolvedRelation)(implicit parser: ExpressionParser) extends TransformationLogicTrait {

  override val transformationName: String = CATALOG_SOURCE_TABLE
  val tableName: String = relation.tableName.toUpperCase
  val dataSourceName = "HIVE TABLE"
  val transformationText: String =
    "SourceType: " + dataSourceName.toUpperCase + "\n" + tableName
}
