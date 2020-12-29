package com.keks.plan.write.types

import org.json4s.JValue


case class JsonPlanType(data: JValue) extends StringPlanType {

  override def asString: String = org.json4s.jackson.compactJson(data)

}
