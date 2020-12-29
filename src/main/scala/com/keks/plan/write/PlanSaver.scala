package com.keks.plan.write

import com.keks.plan.write.types.PlanType


trait PlanSaver[T <: PlanType] {

  def save(data: T,
           path: String,
           fileName: String): Unit

}
