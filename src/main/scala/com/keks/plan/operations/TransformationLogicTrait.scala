package com.keks.plan.operations


trait TransformationLogicTrait {

  /* like join, filter... */
  val transformationName: String

  /* operation description like filter by 'IS NOT NULL'*/
  val transformationText: String

}
