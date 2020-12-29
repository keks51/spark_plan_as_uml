package com.keks.plan.parser

import com.keks.plan.operations._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAlias
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.datasources.LogicalRelation


/**
  * Parsing logical plan to operation node
  */
object OperationParser {

  def parse(nodePlan: LogicalPlan,
            parentPlan: Option[LogicalPlan])(implicit parser: ExpressionParser): PlanOperation = {
    nodePlan match {
      case operation: Deduplicate => DistinctOperationParser(operation)
      case operation: MapElements => MapElementsOperationParser(operation)
      case operation: Filter =>
        // if filter operation is empty like 'filter(identity)' then operation should be skipped
        if (operation.condition.isInstanceOf[AttributeReference])
          SkipOperationParser()
        else
          FilterOperationParser(operation)
      case operation: Join => JoinOperationParser(operation)
      case operation: SubqueryAlias => SubqueryOperationParser(operation)
      case operation: Window => WindowOperationParser(operation)
      case operation: Aggregate => AggregateOperationParser(operation)
      case operation: Union => UnionOperationParser(operation)
      case operation: Generate => GenerateOperationParser(operation)
      case operation: Project =>
        // if parent plan is SubqueryAlias then current plan doesn't contain any useful info
        if (parentPlan.exists(_.isInstanceOf[SubqueryAlias])) {
          SkipOperationParser()
          // if next operation is LocalRelation, LogicalRelation or LogicalRDD then current plan is a Reading plan aka SourceTable like .json(...) or .csv(...)
        } else if (operation.child.isInstanceOf[LocalRelation] || operation.child.isInstanceOf[LogicalRelation] || operation.child.isInstanceOf[LogicalRDD]) {
          SourceTableOperationParser(operation)
          // if project contains only AttributeReferences then this plan is just a projection before join or smth else
        } else if (operation.projectList.forall(_.isInstanceOf[AttributeReference])){
          SkipOperationParser()
        } else if (operation.projectList.forall(_.isInstanceOf[UnresolvedAlias])) {
          SelectOperationParser(operation)
        } else {
          ColumnsModificationsOperationParser(operation)
        }
    }
  }

}
