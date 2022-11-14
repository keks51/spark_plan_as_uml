package com.keks.plan.parser.source

import com.keks.plan.operations.sources.LocalRelationSource
import com.keks.plan.parser.{DefaultExpressionParser, SparkLogicalRelationParser}
import org.apache.spark.sql.catalyst.AliasIdentifier
import utils.TestBase


class SparkDfLogicalParserLocalRelationSourceTest extends TestBase {

  import spark.implicits._

  val planParser = new SparkLogicalRelationParser(new DefaultExpressionParser())

/* LocalRelation [id#2, name#3] */
  "LocalRelationSource" should "test1" in {

    val usersAnalyzed = spark.createDataset(Seq(User(1, "bob")))
      .queryExecution
      .analyzed

    val (datasource, _) = planParser.parse(usersAnalyzed, None)
    assert(datasource.isInstanceOf[LocalRelationSource])
  }


  /* SubqueryAlias `USER`
     +- LocalRelation [id#2, name#3] */
  "LocalRelationSource" should "test2" in {
    val usersAnalyzed = spark.createDataset(Seq(User(1, "bob")))
      .as("USER")
      .queryExecution
      .analyzed

    val alias: AliasIdentifier = planParser
      .parse(usersAnalyzed, None)
      ._1
      .asInstanceOf[LocalRelationSource]
      .name
      .get

    val expectedAlias = AliasIdentifier("USER")

    assert(expectedAlias == alias)
  }

  /* LocalRelation <empty> false */
  "LocalRelationSource" should "test3" in {
    val usersAnalyzed = spark
      .emptyDataFrame
      .queryExecution
      .analyzed

    val (datasource, _) = planParser.parse(usersAnalyzed, None)
    assert(datasource.isInstanceOf[LocalRelationSource])
  }

}
