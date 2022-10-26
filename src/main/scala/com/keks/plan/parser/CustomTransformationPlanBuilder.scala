package com.keks.plan.parser

import com.keks.plan.operations.TransformationLogicTrait
import com.keks.plan.operations.narrow.SkipTransformation
import com.keks.plan.parser
import com.keks.plan.parser.CustomTransformationPlanBuilderUtils._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.datasources.LogicalRelation

import java.util.concurrent.atomic.AtomicInteger


/**
 * Parsing spark plan and building custom plan
 * with detailed information about transformations.
 * During parsing some spark plan nodes should be excluded since
 * they doesn't provide any useful information.
 * This functionality can parse plan crated by Dataframe and spark sql logic.
 * For printing and parsing detailed information special expression parser is used.
 * Spark expression is a function which is used to modify data (cast, sum ...).
 * Expression parser can be changed.
 *
 */
object CustomTransformationPlanBuilder {

  /* List of spark plan relations that should be skipped */
  val defaultExcludeList: Seq[String] = Seq(
    //    classOf[DeserializeToObject].getName,
    //    classOf[LocalRelation].getName,
    //    classOf[LogicalRelation].getName,
    //    classOf[LogicalRDD].getName,
    classOf[Repartition].getName,
    //    classOf[CTERelationRef].getName,
    //    classOf[WithCTE].getName,
    classOf[MapPartitions].getName,
    classOf[ResolvedHint].getName
    //    , classOf[SerializeFromObject].getName
  )

  /**
   * Building custom plan.
   * If plan contains with CTE statements than we have to parse
   * cte and main sql independently.
   * As a result we have CustomTransformationPlan that contains new Tree with custom transformations
   * and lists of nodes and edges.
   *
   * @param rootPlan             spark logical plan
   * @param excludeSparkNodesSeq relations to exclude
   * @param planParser           expression parser
   * @return
   */
  def build(rootPlan: LogicalPlan,
            excludeSparkNodesSeq: Seq[String] = defaultExcludeList,
            planParser: SparkLogicalRelationParser = new SparkLogicalRelationParser(new DefaultExpressionParser())): CustomTransformationPlan = {
    implicit val parser: SparkLogicalRelationParser = planParser
    implicit val excludeSparkNodes: Seq[String] = excludeSparkNodesSeq
    val sparkOperationNodeRoot = build(rootPlan)
    val edges: Array[(Int, Int)] = getEdges(sparkOperationNodeRoot)
    val nodes: Array[TransformationPlanNode] = getNodes[TransformationPlanNode](sparkOperationNodeRoot)
    CustomTransformationPlan(sparkOperationNodeRoot, nodes, edges)
  }

  /**
   * Parsing spark plan without CTE statements.
   * First we parse spark plan and create new tree with detailed transformation information.
   * Next we have to exclude nodes that are not useful or modify some
   * transformations that could not be modified during parsing.
   * Since some of transformations were excluded we have set node ids again.
   *
   * @param rootPlan             spark logical plan
   * @param excludeSparkNodesSeq relations to exclude
   * @param planParser           expression parser
   * @return
   */
  private def build(rootPlan: LogicalPlan)(implicit excludeSparkNodesSeq: Seq[String],
                                           planParser: SparkLogicalRelationParser): TransformationPlanNode = {
    val idCounterWhileParsing = new AtomicInteger(0)
    val sparkOperationTree = parseLogicalPlanRecur(rootPlan, idCounterWhileParsing, parentPlanId = 0, parentLogicalPlan = None)
    val modified = skipAndModifyOperationNodes(sparkOperationTree)
    val correctIds = new AtomicInteger(1)
    val withCorrectIds = setCorrectIds(modified, correctIds)
    withCorrectIds
  }

  /**
   * Recursively parse each spark relation node and get custom transformation node based on relation.
   *
   * @param curPlan           current plan to parse.
   * @param id                AtomicInteger for generating ids
   * @param parentPlanId      previous plan unique operation id
   * @param parentLogicalPlan previous plan
   * @return root node
   */
  private def parseLogicalPlanRecur(curPlan: LogicalPlan,
                                    id: AtomicInteger,
                                    parentPlanId: Int,
                                    parentLogicalPlan: Option[LogicalPlan] = None)(implicit excludeSparkNodesSeq: Seq[String],
                                                                                   planParser: SparkLogicalRelationParser): TransformationPlanNode = {
    val curPlanId = id.getAndIncrement()

    /* Relations that are not useful should be replaced with SkipOperation */
    val (sparkOperation: TransformationLogicTrait, children: Seq[TransformationPlanNode]) = curPlan match {
      /* Converting excluded plan as SkipOperation and parse other */
      case logicalPlan if excludeSparkNodesSeq.contains(logicalPlan.getClass.getName) =>
        val childNodes: Seq[TransformationPlanNode] = logicalPlan
          .children
          .map(childLogicalPlan => parseLogicalPlanRecur(childLogicalPlan, id, parentPlanId, parentLogicalPlan))
        val planOperation: TransformationLogicTrait = SkipTransformation()
        (planOperation, childNodes)

      case logicalPlan =>
        val (planOperation, nextChildren) = planParser.parse(logicalPlan, parentLogicalPlan)

        val childNodesList: Seq[TransformationPlanNode] = nextChildren
          .map { childPlan => parseLogicalPlanRecur(childPlan, id, curPlanId, Some(logicalPlan)) }

        (planOperation, childNodesList)
    }
    parser.TransformationPlanNode(
      parentId = parentPlanId,
      id = curPlanId,
      sparkLogicalPlan = curPlan,
      childrenList = children,
      transformationLogic = sparkOperation,
      parentSparkLogicalPlan = parentLogicalPlan)
  }
  //  private def parseLogicalPlanRecur(curPlan: LogicalPlan,
  //                                    id: AtomicInteger,
  //                                    parentPlanId: Int,
  //                                    parentLogicalPlan: Option[LogicalPlan] = None)(implicit excludeSparkNodesSeq: Seq[String],
  //                                                                                   planParser: SparkLogicalRelationParser): TransformationPlanNode = {
  //    val curPlanId = id.getAndIncrement()
  //
  //    /* Relations that are not useful should be replaced with SkipOperation */
  //    val (sparkOperation, children) = curPlan match {
  //      /* Converting excluded plan as SkipOperation and parse other */
  //      case logicalPlan if excludeSparkNodesSeq.contains(logicalPlan.getClass.getName) =>
  //        val childNodes: Seq[TransformationPlanNode] = logicalPlan
  //          .children
  //          .map(childLogicalPlan => parseLogicalPlanRecur(childLogicalPlan, id, parentPlanId, parentLogicalPlan))
  //        val planOperation: TransformationLogic = SkipOperationParser()
  //        (planOperation, childNodes)
  //
  //      case logicalPlan =>
  //        val childNodesList = logicalPlan
  //          .children
  //          .map { childPlan => parseLogicalPlanRecur(childPlan, id, curPlanId, Some(logicalPlan)) }
  //        val planOperation: TransformationLogic = planParser.parse(logicalPlan, parentLogicalPlan)
  //        (planOperation, childNodesList)
  //    }
  //    TransformationPlanNode(
  //      parentId = parentPlanId,
  //      id = curPlanId,
  //      sparkLogicalPlan = curPlan,
  //      childrenList = children,
  //      transformationLogic = sparkOperation,
  //      parentSparkLogicalPlan = parentLogicalPlan)
  //  }

}
