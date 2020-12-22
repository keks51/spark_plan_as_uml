package com.keks.plan.operations

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.datasources.LogicalRelation


/**
  * Wrapper of spark plan with a reference to previous and next plan or plans.
  * Spark plan is parsed to specific operation with pretty text version of the operation
  * @param parentPlanId previous plan id
  * @param id current plan id
  * @param nodePlan current node plan
  * @param childPlanIds child plan ids
  * @param parentPlan parent spark plan
  */
case class PlanNode(parentPlanId: Int,
                    id: Int,
                    nodePlan: LogicalPlan,
                    childPlanIds: Seq[Int],
                    parentPlan: Option[LogicalPlan]) {

  val planOperation: PlanOperation = nodePlan match {
    case operation: Deduplicate => DistinctOperation(operation)
    case operation: MapElements => MapElementsOperation(operation)
    case operation: Filter =>
      // if filter operation is empty like 'filter(identity)' then operation should be skipped
      if (operation.condition.isInstanceOf[AttributeReference])
        SkipOperation()
      else
        FilterOperation(operation)
    case operation: Join => JoinOperation(operation)
    case operation: SubqueryAlias => SubqueryOperation(operation)
    case operation: Window => WindowOperation(operation)
    case operation: Aggregate => AggregateOperation(operation)
    case operation: Union => UnionOperation(operation)
    case operation: Generate => GenerateOperation(operation)
    case operation: Project =>
      // if parent plan is SubqueryAlias then current plan doesn't contain any useful info
      if (parentPlan.exists(_.isInstanceOf[SubqueryAlias])) {
        SkipOperation()
        // if next operation is LocalRelation, LogicalRelation or LogicalRDD then current plan is a Reading plan aka SourceTable like .json(...) or .csv(...)
      } else if (operation.child.isInstanceOf[LocalRelation] || operation.child.isInstanceOf[LogicalRelation] || operation.child.isInstanceOf[LogicalRDD]) {
        SourceTableOperation(operation)
        // if project contains only AttributeReferences then this plan is just a projection before join or smth else
      } else if (operation.projectList.forall(_.isInstanceOf[AttributeReference])){
        SkipOperation()
      } else {
        ColumnsModificationsOperation(operation)
      }
  }
}