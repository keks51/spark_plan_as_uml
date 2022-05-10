package com.keks.plan.parser

import com.keks.plan.operations.TransformationLogicTrait
import org.apache.spark.sql.catalyst.plans.logical._


/**
 * Wrapper of spark plan with a reference to previous and next plan or plans.
 * Spark plan is parsed to specific operation with pretty text version of the operation
 *
 * @param parentId            previous plan id
 * @param id                               current plan id
 * @param sparkLogicalPlan                 current node spark logical plan
 * @param parentSparkLogicalPlan           parent  spark logical plan
 * @param childrenList list of children's operation plans
 * @param transformationLogic                    operation plan
 */
case class TransformationPlanNode(parentId: Int,
                                  override val id: Int,
                                  sparkLogicalPlan: LogicalPlan,
                                  parentSparkLogicalPlan: Option[LogicalPlan],
                                  override val childrenList: Seq[TransformationPlanNode],
                                  transformationLogic: TransformationLogicTrait) extends TreeNode[TransformationPlanNode](id, childrenList) {

  override def toString: String = s"${transformationLogic.transformationName}, ID:$id"

}

class TreeNode[T <: TreeNode[T]](val id: Int, val childrenList: Seq[T])