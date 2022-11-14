package com.keks.plan.parser.source

import com.keks.plan.operations.sources.OneRowSource
import com.keks.plan.parser.{DefaultExpressionParser, SparkLogicalRelationParser}
import utils.TestBase

class SparkDfLogicalParserOneRowSourceTest extends TestBase {

  val planParser = new SparkLogicalRelationParser(new DefaultExpressionParser())

  //  WithCTE
  //  :- CTERelationDef 0
  //    :  +- SubqueryAlias shit
  //  :     +- Project [1 AS 1#0]
  //  :        +- OneRowRelation
  //    +- Project [1#0]
  //  +- SubqueryAlias shit
  //  +- CTERelationRef 0, true, [1#0]
  "OneRowSource" should "success" in {
    val usersAnalyzed = spark
      .sql(
        """with smth as (select 1)
          |select * from smth""".stripMargin)
      .queryExecution
      .analyzed
    val datasource = planParser.parse(
      planParser.parse(
        planParser.parse(
          planParser.parse(
            planParser.parse(usersAnalyzed, None)._2.head, None)._2.head, None)._2.head, None)._2.head, None)._1
    assert(datasource.isInstanceOf[OneRowSource])

  }
}
