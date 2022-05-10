package com.keks.plan.builder

import com.keks.plan.parser.TransformationPlanNode
import com.keks.plan.write.types.JsonPlanType
import org.json4s.JsonAST._


class JsonDiagramBuilder extends DiagramBuilder[JsonPlanType] {

  override def build(entityName: String,
                     reportDescription: String,
                     savePath: String,
                     planNodes: Seq[TransformationPlanNode],
                     edges: Seq[(Int, Int)]): JsonPlanType = {
    val jsonEdgesArray: JArray = JArray(edges.map(getEdgeAsJson).toList)
    val jsonNodesArray: JArray = JArray(planNodes.map(getNodeAsJson).toList)
    val json: JValue = JObject(
      JField("entityName", JString(entityName)),
      JField("reportDescription", JString(reportDescription)),
      JField("edges", jsonEdgesArray),
      JField("nodes", jsonNodesArray))
    new JsonPlanType(json)
  }

  def getEdgeAsJson(edge: (Int, Int)): JObject = {
    val (from, to) = edge
    JObject(JField("from", JInt(from)), JField("to", JInt(to)))
  }

  def getNodeAsJson(node: TransformationPlanNode): JObject = {
    JObject(
      JField("id", JInt(node.id)),
      JField("name", JString(node.transformationLogic.transformationName)),
      JField("desc", JString(node.transformationLogic.transformationText)))
  }

}
