package com.keks.plan.operations.sources

import com.keks.plan.operations.TransformationLogicTrait
import com.keks.plan.operations.sources.utils.{DataSourceParserUtils, SourceColumn, SourceFilesInfo}
import org.apache.spark.sql.catalyst.AliasIdentifier
import org.apache.spark.sql.catalyst.plans.logical.{OneRowRelation, Project}
import org.apache.spark.sql.execution.datasources.LogicalRelation


case class OneRowSource(oneRowRelation: OneRowRelation) extends TransformationLogicTrait {


  override def equals(obj: Any): Boolean = {
    obj.asInstanceOf[OneRowSource].oneRowRelation == oneRowRelation
  }
  override val transformationName: String = "OneRowRelation"
  override val transformationText: String = oneRowRelation.output.mkString(",")

}
