package com.keks.plan.builder

import com.keks.plan.parser.TransformationPlanNode
import com.keks.plan.write.types.PlantUmlPlanType


/**
  * Generating operations tree as UML report.
  */
class PlanUmlDiagramBuilder(pngLimitSize: Int = 12288) extends DiagramBuilder[PlantUmlPlanType] {

  /**
    * Getting UML report as String
    *
    * @param entityName        result entity name like 'ACCOUNT_TABLE'
    * @param reportDescription like 'pharma users'
    */
  def build(entityName: String,
            reportDescription: String,
            savePath: String,
            planNodes: Seq[TransformationPlanNode],
            edges: Seq[(Int, Int)]): PlantUmlPlanType = {
    System.setProperty("PLANTUML_LIMIT_SIZE", pngLimitSize.toString)
    val umlNodes = getUmlNodes(planNodes)
    val umlEdges = getUmlEdges(edges, planNodes, entityName)
    val res = s"""@startuml
       |!pragma graphviz_dot jdot
       |note top of $entityName
       |  Datamart <b>$entityName</b>.
       |  $reportDescription
       |end note
       |${umlNodes.mkString("\n")}
       |${umlEdges.mkString("\n")}
       |@enduml
       |""".stripMargin
      .replaceAll("`", "")
    PlantUmlPlanType(res)
  }

  private def getUmlNodes(nodes: Seq[TransformationPlanNode]): Seq[String] = {
    nodes.map { node =>
      val id = node.id
      val name = node.transformationLogic.transformationName
      val desc = node.transformationLogic.transformationText
      val umlName = s"${id}_$name"
      s"""class $umlName <<(E,ortho)>> {
         |$desc
         |}
         |hide $umlName circle
       """.stripMargin
    }
  }

  private def getUmlEdges(edges: Seq[(Int, Int)],
                          nodes: Seq[TransformationPlanNode], entityName: String): Seq[String] = {
    edges.map { edge =>
      val fromNodeId = edge._1
      val toNodeId = edge._2
      val fromNodeName = nodes.find(_.id == fromNodeId).get.transformationLogic.transformationName
      val toNodeName = nodes.find(_.id == toNodeId).orElse(throw new Exception(s"Cannot find $toNodeId")).get.transformationLogic.transformationName
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
