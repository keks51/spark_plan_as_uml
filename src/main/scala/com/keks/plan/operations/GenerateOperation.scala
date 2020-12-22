package com.keks.plan.operations

import org.apache.spark.sql.catalyst.plans.logical.Generate


/**
  * Operations like UDF
  * {{{
  *   .withColumn(CALCULATED, explode(customUdf(col(DATA))))
  * }}}
  */
case class GenerateOperation(generate: Generate) extends PlanOperation {

  override val operationName = COLUMNS_MODIFICATIONS

  override val operationText =
    s"${toPrettyExpression(generate.generator, None, withoutTableName = true)} " +
      s"as ${generate.generatorOutput.map(toPrettyExpression(_, None, withoutTableName = true))}"

}
