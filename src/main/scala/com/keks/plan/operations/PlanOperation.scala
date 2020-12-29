package com.keks.plan.operations


trait PlanOperation {

  /* like join, filter... */
  val operationName: String

  /* operation description like filter by 'IS NOT NULL'*/
  val operationText: String

}
