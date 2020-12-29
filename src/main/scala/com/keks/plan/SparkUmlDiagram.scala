package com.keks.plan

import com.keks.plan.operations.PlanNode
import com.keks.plan.parser.ExpressionParser
import net.sourceforge.plantuml.SourceStringReader
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import java.io.File


/**
  * Generating operations tree as UML report.
  */
object SparkUmlDiagram {

  private val PNG_EXTENSION = ".png"

  /**
    * Generating UML report.
    * Getting UML report as string.
    * Rendering UML string and saving.
    *
    * @param entityName        result entity name like 'ACCOUNT_TABLE'
    * @param reportDescription like 'pharma users'
    * @param plan              spark plan
    * @param savePath          save path
    * @param pngLimitSize      picture size
    */
  def buildAndSaveReport(entityName: String,
                         reportDescription: String,
                         plan: LogicalPlan,
                         savePath: String,
                         pngLimitSize: Int = 12288,
                         parser: ExpressionParser): Unit = {
    val strUml: String = buildReport(entityName, reportDescription, plan, pngLimitSize)(parser)
    val outputPath = new File(savePath)
    outputPath.mkdirs()
    val reader = new SourceStringReader(strUml)
    val diagramFile = new File(s"$outputPath/$entityName$PNG_EXTENSION")
    Option(reader.generateImage(diagramFile))
      .orElse(throw new Exception(s"Cannot generate diagram file for entity $entityName"))
  }

  /**
    * Getting UML report as String
    *
    * @param entityName        result entity name like 'ACCOUNT_TABLE'
    * @param reportDescription like 'pharma users'
    * @param plan              spark plan
    * @param pngLimitSize      picture size
    */
  private def buildReport(entityName: String,
                          reportDescription: String,
                          plan: LogicalPlan,
                          pngLimitSize: Int = 12288)(implicit parser: ExpressionParser): String = {
    System.setProperty("PLANTUML_LIMIT_SIZE", pngLimitSize.toString)

    val planNodes: Seq[PlanNode] = SparkPlanParser(plan).parse()
    val edgesWithExistNodes = SparkPlanParser.getEdges(planNodes)

    val umlNodes = getUmlNodes(planNodes)
    val umlEdges = getUmlEdges(edgesWithExistNodes, planNodes, entityName)

    s"""@startuml
       |!pragma graphviz_dot jdot
       |note top of $entityName
       |  Datamart <b>$entityName</b> $reportDescription
       |end note
       |${umlNodes.mkString("\n")}
       |${umlEdges.mkString("\n")}
       |@enduml
       |""".stripMargin
      .replaceAll("`", "")
  }

  private def getUmlNodes(nodes: Seq[PlanNode]): Seq[String] = {
    nodes.map { node =>
      val id = node.id
      val name = node.planOperation.operationName
      val desc = node.planOperation.operationText
      val umlName = s"${id}_$name"
      s"""class $umlName <<(E,ortho)>> {
         |$desc
         |}
         |hide $umlName circle
       """.stripMargin
    }
  }

  private def getUmlEdges(edges: Seq[(Int, Int)],
                          nodes: Seq[PlanNode], entityName: String): Seq[String] = {
    edges.map { edge =>
      val fromNodeId = edge._1
      val toNodeId = edge._2
      val fromNodeName = nodes.find(_.id == fromNodeId).get.planOperation.operationName
      val toNodeName = nodes.find(_.id == toNodeId).orElse(throw new Exception(s"Cannot find $toNodeId")).get.planOperation.operationName
      val umlFrom = s"${fromNodeId}_$fromNodeName"
      if (fromNodeId == 1) {
        s"""$entityName||--|| $umlFrom
                       |$umlFrom||--|| ${toNodeId}_$toNodeName""".stripMargin
      } else {
        s"$umlFrom ||--|| ${toNodeId}_$toNodeName"
      }
    }
  }

}
