package com.keks.plan.operations

import com.keks.plan.parser.{ExpressionParser, OperationParser}
import org.apache.spark.sql.catalyst.plans.logical._


/**
  * Wrapper of spark plan with a reference to previous and next plan or plans.
  * Spark plan is parsed to specific operation with pretty text version of the operation
  * @param parentPlanId previous plan id
  * @param id current plan id
  * @param nodePlan current node plan
  * @param childPlanIds child plan ids
  * @param parentPlan parent spark plan
  * @param parser custom parser of the spark expressions
  */
case class PlanNode(parentPlanId: Int,
                    id: Int,
                    nodePlan: LogicalPlan,
                    childPlanIds: Seq[Int],
                    parentPlan: Option[LogicalPlan])(implicit parser: ExpressionParser) {

  val planOperation: PlanOperation = OperationParser.parse(nodePlan, parentPlan)

}