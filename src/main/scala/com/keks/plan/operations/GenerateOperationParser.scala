package com.keks.plan.operations

import com.keks.plan.parser.ExpressionParser
import org.apache.spark.sql.catalyst.plans.logical.Generate


/**
  * Operations like UDF
  * {{{
  *   .withColumn(CALCULATED, explode(customUdf(col(DATA))))
  * }}}
  */
case class GenerateOperationParser(generate: Generate)(implicit parser: ExpressionParser) extends PlanOperation {

  override val operationName = COLUMNS_MODIFICATIONS

  override val operationText =
    s"${parser.toPrettyExpression(generate.generator, None, withoutTableName = true)} " +
      s"as ${generate.generatorOutput.map(parser.toPrettyExpression(_, None, withoutTableName = true))}"

}
