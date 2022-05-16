package com.keks.plan.parser.narrow

import com.keks.plan.operations.COLUMNS_MODIFICATIONS
import com.keks.plan.parser.{DefaultExpressionParser, SparkLogicalRelationParser}
import org.apache.spark.sql.functions.{col, explode}
import utils.TestBase


class SparkDfLogicalParserGenerateTest extends TestBase {

  import spark.implicits._

  val planParser = new SparkLogicalRelationParser(new DefaultExpressionParser())


  /* Project [id#5, names#6, name#10]
     +- Generate explode(names#6), false, [name#10]
        +- Project [_1#2 AS id#5, _2#3 AS names#6]
           +- LocalRelation [_1#2, _2#3] */
  "Generate" should "test1" in {
    val usersAnalyzed = Seq((1, Array("alex", "bob")))
      .toDF("id", "names")
      .withColumn("name", explode(col("names")))
      .queryExecution.analyzed

    val (transformation1, _) = planParser.parse(usersAnalyzed, None)

    assert(transformation1.transformationName == COLUMNS_MODIFICATIONS)
  }

}
