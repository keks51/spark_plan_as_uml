package com.keks.plan.parser.wide

import com.keks.plan.operations.DEDUPLICATE
import com.keks.plan.parser.{DefaultExpressionParser, SparkLogicalRelationParser}
import utils.TestBase


class SparkDfLogicalParserDeduplicateTest extends TestBase {

  import spark.implicits._

  val planParser = new SparkLogicalRelationParser(new DefaultExpressionParser())


  /* Deduplicate [city#5, salary#6]
     +- Project [_1#2 AS city#5, _2#3 AS salary#6]
        +- LocalRelation [_1#2, _2#3] */
  "Deduplicate" should "test1" in {
    val cityAndSalaryAnalyzed = Seq(("moscow", 10.0))
      .toDF("city", "salary")
      .distinct()
      .queryExecution.analyzed

    val (transformation, _) = planParser.parse(cityAndSalaryAnalyzed, None)

    assert(transformation.transformationName == DEDUPLICATE)
  }


  /* Deduplicate [city#5]
     +- Project [_1#2 AS city#5, _2#3 AS salary#6]
        +- LocalRelation [_1#2, _2#3] */
  "Deduplicate" should "test2" in {
    val cityAndSalaryAnalyzed = Seq(("moscow", 10.0))
      .toDF("city", "salary")
      .dropDuplicates("city")
      .queryExecution.analyzed

    val (transformation, _) = planParser.parse(cityAndSalaryAnalyzed, None)

    assert(transformation.transformationName == DEDUPLICATE)
  }


  /* Deduplicate [city#5, salary#6]
     +- Project [city#5, salary#6]
        +- Project [_1#2 AS city#5, _2#3 AS salary#6]
           +- LocalRelation [_1#2, _2#3] */
  "Deduplicate" should "test3" in {
    val cityAndSalaryAnalyzed = Seq(("moscow", 10.0))
      .toDF("city", "salary")
      .select("city", "salary")
      .distinct()
      .queryExecution.analyzed

    val (transformation, _) = planParser.parse(cityAndSalaryAnalyzed, None)

    assert(transformation.transformationName == DEDUPLICATE)
  }


  /* Deduplicate [city#5]
     +- Project [city#5, salary#6]
        +- Project [_1#2 AS city#5, _2#3 AS salary#6]
           +- LocalRelation [_1#2, _2#3] */
  "Deduplicate" should "test4" in {
    val cityAndSalaryAnalyzed = Seq(("moscow", 10.0))
      .toDF("city", "salary")
      .select("city", "salary")
      .dropDuplicates("city")
      .queryExecution.analyzed

    val (transformation, _) = planParser.parse(cityAndSalaryAnalyzed, None)

    assert(transformation.transformationName == DEDUPLICATE)
  }


}
