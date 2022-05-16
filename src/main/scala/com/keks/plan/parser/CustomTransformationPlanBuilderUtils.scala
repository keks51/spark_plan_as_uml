package com.keks.plan.parser

import com.keks.plan.operations.narrow.{CatalogTableTransformation, SkipTransformation}
import com.keks.plan.operations.sources
import com.keks.plan.operations.sources.WithTableTransformation

import java.util.concurrent.atomic.AtomicInteger
import scala.reflect.ClassTag


object CustomTransformationPlanBuilderUtils {

  /**
   * Getting List of edges.
   */
  def getEdges[T <: TreeNode[T]](nodes: TreeNode[T]): Array[(Int, Int)] = {
    getEdgesRecur(nodes).distinct.toArray
  }

  def getEdgesRecur[T <: TreeNode[T]](transformationNode: TreeNode[T]): Seq[(Int, Int)] = {
    val x: Seq[(Int, Int)] = transformationNode.childrenList.map(e => (transformationNode.id, e.id))
    val y: Seq[(Int, Int)] = transformationNode.childrenList.flatMap { e =>
      getEdgesRecur(e)
    }
    x ++ y
  }

  def getNodes[T <: TreeNode[T] : ClassTag](planNode: T): Array[T] = {
    getNodesRecur(planNode).distinct.toArray
  }

  def getNodesRecur[T <: TreeNode[T]](planNode: T): Seq[T] = {
    val x: Seq[T] = Seq(planNode)
    val y: Seq[T] = planNode.childrenList.flatMap(getNodesRecur)
    val z = x ++ y
    z
  }

  def setCorrectIds(operationPlanNode: TransformationPlanNode, nextId: AtomicInteger): TransformationPlanNode = {
    val parentId = nextId.get()
    val id = nextId.getAndIncrement()
    val children = operationPlanNode.childrenList.map { child =>
      setCorrectIds(child, nextId)
    }

    operationPlanNode.copy(
      parentId = parentId,
      id = id,
      childrenList = children)
  }

  def skipAndModifyOperationNodes(operationPlanNode: TransformationPlanNode): TransformationPlanNode = {
    val modifiedNodeHead = recursivelyRemoveSkipOperationNodes(operationPlanNode)
    modifiedNodeHead.transformationLogic match {
      case SkipTransformation() => modifiedNodeHead.childrenList.head
      case _ => modifiedNodeHead
    }
  }

  def recursivelyRemoveSkipOperationNodes(operationPlanNode: TransformationPlanNode): TransformationPlanNode = {
    val children = operationPlanNode.childrenList.flatMap { nextPlan =>
      nextPlan.transformationLogic match {
        case SkipTransformation() =>
          recursivelyRemoveSkipOperationNodes(nextPlan).childrenList
        case _ =>
          Seq(recursivelyRemoveSkipOperationNodes(nextPlan))
      }
    }
    operationPlanNode.copy(childrenList = children)
  }

  def addCteToMainPlain(mainPlan: TransformationPlanNode, ctes: Map[String, TransformationPlanNode]): TransformationPlanNode = {
    mainPlan.transformationLogic match {
      case catalog: CatalogTableTransformation =>
        ctes.get(catalog.tableName)
          .map(e => mainPlan.copy(childrenList = Seq(e), transformationLogic = sources.WithTableTransformation(catalog.relation)))
          .getOrElse(mainPlan)
      case _ =>
        mainPlan
          .copy(childrenList = mainPlan.childrenList.map(addCteToMainPlain(_, ctes)))
    }
  }

}
