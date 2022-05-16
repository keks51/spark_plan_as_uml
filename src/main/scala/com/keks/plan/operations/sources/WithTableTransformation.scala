package com.keks.plan.operations.sources

import com.keks.plan.operations.{TransformationLogicTrait, WITH_TABLE}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation


case class WithTableTransformation(relation: UnresolvedRelation) extends TransformationLogicTrait {
  override val transformationName: String = WITH_TABLE
  val tableName: String = relation.tableName
  val transformationText: String = tableName
}
