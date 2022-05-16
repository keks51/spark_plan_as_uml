package com.keks.plan.operations.narrow

import com.keks.plan.operations.{SKIP_OPERATION, TransformationLogicTrait}

/**
 * Skip operation. Just a mock.
 */
case class SkipTransformation() extends TransformationLogicTrait {

  override val transformationName: String = SKIP_OPERATION

  override val transformationText: String = ""

}
