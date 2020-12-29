package com.keks.plan.write.types


case class PlantUmlPlanType(data: String) extends PlanType {

  override def asString = data

}
