package com.keks.plan

import com.keks.plan.SparkPlanParser.{defaultExcludeList, getEdges}
import com.keks.plan.operations.{PlanNode, SKIP_OPERATION}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.datasources.LogicalRelation

import java.util.concurrent.atomic.AtomicInteger


/**
  * Parsing spark plan as a tree of operations.
  *
  * @param rootPlan             spark plan
  * @param excludeSparkNodesSeq list of spark plans to exclude like (DeserializeToObject)
  */
class SparkPlanParser(rootPlan: LogicalPlan, excludeSparkNodesSeq: Seq[String] = defaultExcludeList) {

  /* unique operation id */
  private val id = new AtomicInteger(1)

  private def nextId: Int = id.getAndIncrement()

  /**
    * Parsing spark plan.
    * Skipping useless operations,
    *
    * @return sequence of operations
    */
  def parse(): Seq[PlanNode] = {
    id.set(1)
    val planNodes = parseRecur(rootPlan, nextId, 0, None)

    filterSkipNodes(planNodes)

  }

  /**
    * Recursively parse each spark plan.
    * @param curPlan current plan to parse.
    * @param curPlanId current plan unique operation id
    * @param parentPlanId previous plan unique operation id
    * @param parentPlan previous plan
    * @return sequence of operations
    */
  private def parseRecur(curPlan: LogicalPlan,
                         curPlanId: Int,
                         parentPlanId: Int,
                         parentPlan: Option[LogicalPlan]): Seq[PlanNode] = {
    curPlan match {
      /* Converting excluded plan as SkipOperation and parse other */
      case logicalPlan if excludeSparkNodesSeq.contains(logicalPlan.getClass.getName) =>
        val childNodes: Seq[Seq[PlanNode]] = logicalPlan
          .children
          .map(childPlan => parseRecur(childPlan, curPlanId, parentPlanId, parentPlan))
        childNodes.flatten

      case logicalPlan =>
        val (childPlanIds, childNodes) = logicalPlan.children.map { childPlan =>
          val childPlanId = nextId
          (childPlanId, parseRecur(childPlan, childPlanId, curPlanId, Some(logicalPlan)))
        }.unzip

        PlanNode(
          parentPlanId = parentPlanId,
          id = curPlanId,
          nodePlan = curPlan,
          childPlanIds = childPlanIds,
          parentPlan = parentPlan) +: childNodes.flatten
    }
  }

  /**
    * Excluding Skip operations.
    * @param planNodes sequence of operations
    * @return filtered sequence of operations
    */
  def filterSkipNodes(planNodes: Seq[PlanNode]): Seq[PlanNode] = {
    val edges = getEdges(planNodes)
    val res = edges.find(e => planNodes.find(_.id == e._2).exists(_.planOperation.operationName == SKIP_OPERATION))

    val newRes = for {
      (fromId, toId) <- res
      fromNode <- planNodes.find(_.id == fromId)
      skipNode <- planNodes.find(_.id == toId)
    } yield {
      val skipNodeChildIds = skipNode.childPlanIds
      val skipNodesChildren = planNodes.filter(_.parentPlanId == skipNode.id)
      val modifiedFromNode = fromNode.copy(childPlanIds = skipNodeChildIds ++ fromNode.childPlanIds)
      val modifiedSkipNodesChildren = skipNodesChildren.map(_.copy(parentPlanId = fromNode.id))
      val allModifiedNodesIds = Seq(fromId, skipNode.id) ++ skipNodesChildren.map(_.id)
      val otherNodes = planNodes.filterNot(e => allModifiedNodesIds.contains(e.id))
      (otherNodes ++ modifiedSkipNodesChildren :+ modifiedFromNode).sortBy(_.id)
    }
    newRes.map(filterSkipNodes).getOrElse(planNodes)
  }

}

object SparkPlanParser {

  val defaultExcludeList: Seq[String] = Seq(
    classOf[DeserializeToObject].getName,
    classOf[LocalRelation].getName,
    classOf[LogicalRelation].getName,
    classOf[LogicalRDD].getName,
    classOf[Repartition].getName,
    classOf[MapPartitions].getName,
    classOf[ResolvedHint].getName,
    classOf[SerializeFromObject].getName)

  def apply(rootPlan: LogicalPlan,
            excludeSparkNodesSeq: Seq[String] = defaultExcludeList): SparkPlanParser =
    new SparkPlanParser(rootPlan, excludeSparkNodesSeq)

  /**
    * Getting List of edges.
    */
  def getEdges(planNodes: Seq[PlanNode]): Seq[(Int, Int)] = {
    val edges = planNodes
      .map(e => (e.id, e.childPlanIds))
      .flatMap(e => e._2.map((e._1, _)))

    edges.filter(e => edges.map(_._1).contains(e._2))
  }

}
