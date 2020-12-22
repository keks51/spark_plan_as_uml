package com.keks.plan.operations

/**
  * Skip operation. Just a mock.
  */
case class SkipOperation() extends PlanOperation {

  override val operationName = SKIP_OPERATION

  override val operationText = ""

}