package com.keks.plan.builder

import com.keks.plan.operations.PlanNode
import com.keks.plan.write.types.PlanType


trait DiagramBuilder[T <: PlanType] {

  def build(entityName: String,
            reportDescription: String,
            savePath: String,
            planNodes: Seq[PlanNode],
            edges: Seq[(Int, Int)]): T


}
