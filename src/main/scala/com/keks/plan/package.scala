package com.keks

import com.keks.plan.builder.DiagramBuilder
import com.keks.plan.operations.PlanNode
import com.keks.plan.parser.{DefaultExpressionParser, ExpressionParser, SparkPlanParser}
import com.keks.plan.write.PlanSaver
import com.keks.plan.write.types.PlanType
import org.apache.spark.sql.Dataset


package object plan {

  /* Invisible space character */
  val INV: String = Character.toString(10240)
  val INV3: String = s"$INV$INV$INV"
  val INV6: String = s"$INV3$INV3"
  val INV9: String = s"$INV6$INV3"

  /**
    * Pretty print methods
    */
  implicit class ParserImp[T](x: Dataset[T]) {

    def printPlan[S <: PlanType](entityName: String,
                                reportDescription: String,
                                savePath: String,
                                parser: ExpressionParser = new DefaultExpressionParser,
                                builder: DiagramBuilder[S],
                                saver: PlanSaver[S]): Unit = {
      val planNodes: Seq[PlanNode] = SparkPlanParser(x.queryExecution.logical)(parser).parse()
      val edges: Seq[(Int, Int)] = SparkPlanParser.getEdges(planNodes)
      val data: S = builder.build(savePath = savePath,
                                  entityName = entityName,
                                  reportDescription = reportDescription,
                                  planNodes = planNodes,
                                  edges = edges)
      saver.save(data, savePath, entityName)
    }

  }

  implicit class StringOps(seq: Seq[String]) {

    /**
      * In case of multiply columns a good decision is to group them in several strings
      * For instance 'a,b,c,d' is better to print as
      * 'a,b'
      * 'c,d'
      *
      * @param size group size
      */
    def groupedBy(size: Int): Seq[String] = {
      seq.grouped(size).map(_.mkString(",")).toSeq
    }

  }

}
