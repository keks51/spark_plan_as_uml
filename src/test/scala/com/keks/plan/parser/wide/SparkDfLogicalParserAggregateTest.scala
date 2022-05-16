package com.keks.plan.parser.wide

import com.keks.plan.operations.AGGREGATE
import com.keks.plan.parser.{DefaultExpressionParser, SparkLogicalRelationParser}
import org.apache.spark.sql.functions._
import utils.TestBase


class SparkDfLogicalParserAggregateTest extends TestBase {

  import spark.implicits._

  val planParser = new SparkLogicalRelationParser(new DefaultExpressionParser())


  /* Aggregate [city#5], [city#5, avg(salary#6) AS avg_salary#12]
     +- Project [_1#2 AS city#5, _2#3 AS salary#6]
        +- LocalRelation [_1#2, _2#3] */
  "Aggregate" should "test1" in {
    val cityAndSalaryAnalyzed = Seq(("moscow", 10.0))
      .toDF("city", "salary")
      .groupBy("city").agg(avg("salary").as("avg_salary"))
      .queryExecution.analyzed

    val (res1, _) = planParser.parse(cityAndSalaryAnalyzed, None)

    assert(res1.transformationName == AGGREGATE)
  }

}
