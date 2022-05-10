package com.keks.plan.parser.wide

import com.keks.plan.operations.WINDOW
import com.keks.plan.parser.{DefaultExpressionParser, SparkLogicalRelationParser}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import utils.TestBase


class SparkDfLogicalParserWindowTest extends TestBase {

  import spark.implicits._

  val planParser = new SparkLogicalRelationParser(new DefaultExpressionParser())


  /* Project [city#5, salary#6, avg_salary#10]
     +- Project [city#5, salary#6, avg_salary#10, avg_salary#10]
        +- Window [avg(salary#6) windowspecdefinition(city#5, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS avg_salary#10], [city#5]
           +- Project [city#5, salary#6]
              +- Project [_1#2 AS city#5, _2#3 AS salary#6]
                 +- LocalRelation [_1#2, _2#3] */
  "WindowTransformation" should "test1" in {
    val cityAndSalaryAnalyzed = Seq(("moscow", 10.0))
      .toDF("city", "salary")
      .withColumn("avg_salary", avg("salary").over(Window.partitionBy("city"))).queryExecution.analyzed

    val (res1, _) = planParser.parse(cityAndSalaryAnalyzed, None)

    assert(res1.transformationName == WINDOW)
  }


  /* Project [city#5, avg(salary) OVER (PARTITION BY city unspecifiedframe$())#10]
     +- Project [city#5, salary#6, avg(salary) OVER (PARTITION BY city unspecifiedframe$())#10, avg(salary) OVER (PARTITION BY city unspecifiedframe$())#10]
        +- Window [avg(salary#6) windowspecdefinition(city#5, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS avg(salary) OVER (PARTITION BY city unspecifiedframe$())#10], [city#5]
           +- Project [city#5, salary#6]
              +- Project [_1#2 AS city#5, _2#3 AS salary#6]
                 +- LocalRelation [_1#2, _2#3] */
  "WindowTransformation" should "test2" in {
    val cityAndSalaryAnalyzed = Seq(("moscow", 10.0))
      .toDF("city", "salary")
      .select(col("city"), avg("salary").over(Window.partitionBy("city")))
      .queryExecution.analyzed

    val (res1, _) = planParser.parse(cityAndSalaryAnalyzed, None)

    assert(res1.transformationName == WINDOW)
  }


}
