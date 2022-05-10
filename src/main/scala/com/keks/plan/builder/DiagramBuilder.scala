package com.keks.plan.builder

import com.keks.plan.parser.TransformationPlanNode
import com.keks.plan.write.types.PlanType


trait DiagramBuilder[T <: PlanType] {

  def build(entityName: String,
            reportDescription: String,
            savePath: String,
            planNodes: Seq[TransformationPlanNode],
            edges: Seq[(Int, Int)]): T


}
