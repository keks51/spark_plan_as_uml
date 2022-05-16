package com.keks.plan.parser.narrow

import com.keks.plan.operations.FILTER
import com.keks.plan.parser.{DefaultExpressionParser, SparkLogicalRelationParser}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.functions.col
import utils.TestBase


class SparkDfLogicalParserFilterTest extends TestBase {

  import spark.implicits._

  val planParser = new SparkLogicalRelationParser(new DefaultExpressionParser())


  /* Filter (city#5 = moscow)
     +- Project [_1#2 AS city#5, _2#3 AS salary#6]
        +- LocalRelation [_1#2, _2#3] */
  "Filter" should "test1" in {
    val cityAndSalaryAnalyzed: LogicalPlan = Seq(("moscow", 10.0))
      .toDF("city", "salary")
      .filter(col("city") === "moscow")
      .queryExecution.analyzed

    val (res, _) = planParser.parse(cityAndSalaryAnalyzed, None)

    assert(res.transformationName == FILTER)
  }


  /* Filter (city#5 = moscow)
     +- Project [_1#2 AS city#5, _2#3 AS salary#6]
        +- LocalRelation [_1#2, _2#3] */
  "Filter" should "test2" in {
    val cityAndSalaryAnalyzed = Seq(("moscow", 10.0))
      .toDF("city", "salary")
      .filter("city == 'moscow'")
      .queryExecution.analyzed

    val (res, _) = planParser.parse(cityAndSalaryAnalyzed, None)

    assert(res.transformationName == FILTER)
  }

}
