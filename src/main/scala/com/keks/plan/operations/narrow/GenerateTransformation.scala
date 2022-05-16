package com.keks.plan.operations.narrow

import com.keks.plan.operations.{COLUMNS_MODIFICATIONS, TransformationLogicTrait}
import com.keks.plan.parser.ExpressionParser
import org.apache.spark.sql.catalyst.plans.logical.Generate

/**
 * Operations like UDF
 * {{{
 *   .withColumn(CALCULATED, explode(customUdf(col(DATA))))
 * }}}
 */
case class GenerateTransformation(generate: Generate)(implicit parser: ExpressionParser) extends TransformationLogicTrait {

  override val transformationName: String = COLUMNS_MODIFICATIONS

  override val transformationText: String =
    s"${parser.toPrettyExpression(generate.generator, None, withoutTableName = true)} " +
      s"as ${generate.generatorOutput.map(parser.toPrettyExpression(_, None, withoutTableName = true))}"

}
