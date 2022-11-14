package com.keks.plan.parser.narrow

import com.keks.plan.operations.narrow.SkipTransformation
import com.keks.plan.operations.sources.OneRowSource
import com.keks.plan.parser.{DefaultExpressionParser, SparkLogicalRelationParser}
import utils.TestBase

class SparkDfLogicalParserViewTransformationTest extends TestBase {

  val planParser = new SparkLogicalRelationParser(new DefaultExpressionParser())

  //  View
  //  Project [a#20, b#21, c#22, d#23]
  //+- SubqueryAlias spark_catalog.default.v
  //   +- View (`default`.`v`, [a#20,b#21,c#22,d#23])
  //      +- Project [cast(a#16 as int) AS a#20, cast(b#17 as int) AS b#21, cast(c#18 as int) AS c#22, cast(d#19 as int) AS d#23]
  //         +- Project [1 AS a#16, 2 AS b#17, 3 AS c#18, 4 AS d#19]
  //            +- OneRowRelation
  "ViewSource" should "success" in {
    val viewSource = spark.sql(
      """CREATE VIEW v AS
        |    SELECT 1 as a, 2 as b, 3 as c, 4 as d""".stripMargin)
    val usersAnalyzed = spark
      .sql(
        """select * from v""".stripMargin)
      .queryExecution
      .analyzed
    val datasource = planParser.parse(
      planParser.parse(
        planParser.parse(usersAnalyzed, None)._2.head, None
      )
        ._2.head, None
    )._1
    assert(datasource.isInstanceOf[SkipTransformation])

  }
}
