package com.keks.plan.parser

import com.keks.plan.operations.{narrow, wide, _}
import com.keks.plan.operations.narrow.{AliasTransformation, CatalogTableTransformation, FilterTransformation, FlatMapTransformation, GenerateTransformation, MapTransformation, SelectTransformation, SkipTransformation, UnionTransformation, WithColumnsTransformation}
import com.keks.plan.operations.sources.utils.DataSourceParserUtils
import com.keks.plan.operations.sources.{CreateDataframeSource, LocalRelationSource, ToDfSource}
import com.keks.plan.operations.wide.{AggregateTransformation, DeduplicateTransformation, DistinctTransformation, JoinTransformation, WindowTransformation}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.datasources.LogicalRelation


/**
  * Parsing logical plan to operation node
  */
class SparkLogicalRelationParser(expressionParser: ExpressionParser) {

  implicit val parser: ExpressionParser = expressionParser

  def parse(nodePlan: LogicalPlan,
            parentPlan: Option[LogicalPlan]): (TransformationLogicTrait, Seq[LogicalPlan]) = {
    nodePlan match {
      case deduplicate: Deduplicate => (DeduplicateTransformation(deduplicate), deduplicate.children)
      case distinct: Distinct => (DistinctTransformation(distinct), distinct.children) // TODO NOT TESTED

      case SerializeFromObject(_, mapElements@MapElements(_,_,_,_,DeserializeToObject(_, _, child))) =>
        (MapTransformation(mapElements), Seq(child))
      case SerializeFromObject(_, mapPartitions@MapPartitions(_,_,DeserializeToObject(_, _, child))) =>
        (FlatMapTransformation(mapPartitions), Seq(child))
      case DeserializeToObject(_, _, child) => (SkipTransformation(), Seq(child))
      case SerializeFromObject(_, child) => (SkipTransformation(), Seq(child))

      case mapElements: MapElements => (narrow.MapTransformation(mapElements), mapElements.children)

      case filter: Filter =>
        // if filter operation is empty like 'filter(identity)' then operation should be skipped
        if (filter.condition.isInstanceOf[AttributeReference])  // TODO maybe should be removed
          (SkipTransformation(), filter.children)
        else
          (FilterTransformation(filter), filter.children)

      case subqueryAlias: SubqueryAlias if subqueryAlias.name.identifier == "__auto_generated_subquery_name" =>
        (SkipTransformation(), subqueryAlias.children)
      case SubqueryAlias(name, p@Project(_, _: LocalRelation)) if containsOnlyAttRefOrAlias(p.projectList) =>
        (ToDfSource(p, Some(name)), p.children.flatMap(_.children))
      case SubqueryAlias(name, p@Project(_, _: LogicalRDD)) if containsOnlyAttRefOrAlias(p.projectList) =>
        (ToDfSource(p, Some(name)), p.children.flatMap(_.children))
      case SubqueryAlias(name, p@Project(_, logicalRelation: LogicalRelation)) if containsOnlyAttRefOrAlias(p.projectList) =>
        (DataSourceParserUtils.parseSource(logicalRelation, Some(p), Some(name)), p.children.flatMap(_.children))
      case SubqueryAlias(name, logicalRDD: LogicalRDD) =>
        (CreateDataframeSource(logicalRDD, Some(name)), logicalRDD.children)
      case SubqueryAlias(name, localRelation: LocalRelation) =>
        (LocalRelationSource(localRelation, Some(name)), localRelation.children)

      case subqueryAlias: SubqueryAlias => (AliasTransformation(subqueryAlias), subqueryAlias.children)

      case Project(_, join: Join) => (JoinTransformation(join), join.children)
      case join: Join => (wide.JoinTransformation(join), join.children)

      case Project(_, Project(_, window: Window)) => (WindowTransformation(window), window.children)
      case Project(_, window: Window) => (wide.WindowTransformation(window), window.children)
      case window: Window => (wide.WindowTransformation(window), window.children)

      case agg: Aggregate => (AggregateTransformation(agg), agg.children)
      case union: Union => (UnionTransformation(union), union.children)

      case Project(_, generate: Generate) => (GenerateTransformation(generate), generate.children)
      case generate: Generate => (narrow.GenerateTransformation(generate), generate.children)

      case p@Project(_, logicalRelation: LogicalRelation) if containsOnlyAttRefOrAlias(p.projectList) =>
        (DataSourceParserUtils.parseSource(logicalRelation, Some(p)), p.children.flatMap(_.children))
      case logicalRelation: LogicalRelation =>
        (DataSourceParserUtils.parseSource(logicalRelation), logicalRelation.children)

      case p@Project(_, _: LocalRelation) if containsOnlyAttRefOrAlias(p.projectList) =>
        (ToDfSource(p), p.children.flatMap(_.children))
      case localRelation: LocalRelation =>
        (LocalRelationSource(localRelation), nodePlan.children.flatMap(_.children))

      case p@Project(_, _: LogicalRDD) if containsOnlyAttRefOrAlias(p.projectList) =>
        (ToDfSource(p), p.children.flatMap(_.children))
      case logicalRDD: LogicalRDD =>
        (CreateDataframeSource(logicalRDD), logicalRDD.children)

      case p@Project(projectList, _) if containsOnlyAttRef(projectList) => (SelectTransformation(p), p.children)
      case p@Project(projectList, _) if !containsOnlyAttRef(projectList) => (WithColumnsTransformation(p), p.children)

      case operation: UnresolvedRelation =>
        (CatalogTableTransformation(operation), nodePlan.children)

      case operation: With =>
        throw new NotImplementedError("'With' cte is not implemented")
    }
  }

  def containsOnlyAttRef(arr: Seq[NamedExpression]): Boolean = {
    arr.map(_.isInstanceOf[AttributeReference]).reduce(_ && _)
  }

  def containsOnlyAttRefOrAlias(arr: Seq[NamedExpression]): Boolean = {
    arr.map(e => e.isInstanceOf[AttributeReference] ||
      (e.isInstanceOf[Alias] && e.asInstanceOf[Alias].child.isInstanceOf[AttributeReference])).reduce(_ && _)
  }

}
