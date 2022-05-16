package com.keks.plan.parser.source

import com.keks.plan.operations.sources.CreateDataframeSource
import com.keks.plan.parser.{DefaultExpressionParser, SparkLogicalRelationParser}
import org.apache.spark.sql.catalyst.AliasIdentifier
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{Encoders, Row}
import utils.TestBase


class SparkDfLogicalParserCreateDataFrameSourceTest extends TestBase {

  val planParser = new SparkLogicalRelationParser(new DefaultExpressionParser())


/* LogicalRDD false */
  "CreateDataFrameSource" should "test1" in {
    val usersAnalyzed = spark
      .emptyDataFrame
      .queryExecution
      .analyzed

    val (datasource, _) = planParser.parse(usersAnalyzed, None)
    assert(datasource.isInstanceOf[CreateDataframeSource])
  }


/* LogicalRDD [id#4, name#5], false */
  "CreateDataFrameSource" should "test2" in {
    val schema = Encoders.product[User].schema
    val usersAnalyzed = spark
      .createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
      .queryExecution
      .analyzed

    val (datasource, _) = planParser.parse(usersAnalyzed, None)
    assert(datasource.isInstanceOf[CreateDataframeSource])
  }


/* LogicalRDD [id#4, name#5], false */
  "CreateDataFrameSource" should "test3" in {
    val schema = Encoders.product[User].schema
    val usersAnalyzed = spark
      .createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
      .queryExecution
      .analyzed

    val resColumns = planParser
      .parse(usersAnalyzed, None)
      ._1
      .asInstanceOf[CreateDataframeSource]
      .sourceColumns
      .map(e => (e.name, e.dataType))

    val expectedColumns = Array(
      ("id", IntegerType),
      ("name", StringType)
    )
    expectedColumns should contain theSameElementsAs resColumns
  }


  /* SubqueryAlias `USER`
     +- LogicalRDD [id#4, name#5], false */
  "CreateDataFrameSource" should "test5" in {
    val schema = Encoders.product[User].schema
    val usersAnalyzed = spark
      .createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
      .as("USER")
      .queryExecution
      .analyzed

    val alias: AliasIdentifier = planParser
      .parse(usersAnalyzed, None)
      ._1
      .asInstanceOf[CreateDataframeSource]
      .name
      .get

    val expectedAlias = AliasIdentifier("USER")

    assert(expectedAlias == alias)
  }


  /* SubqueryAlias `USER`
     +- LogicalRDD [id#4, name#5], false */
  "CreateDataFrameSource" should "test6" in {
    val schema = Encoders.product[User].schema
    val usersAnalyzed = spark
      .createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
      .as("USER")
      .queryExecution
      .analyzed

    val alias: AliasIdentifier = planParser
      .parse(usersAnalyzed, None)
      ._1
      .asInstanceOf[CreateDataframeSource]
      .name
      .get

    val expectedAlias = AliasIdentifier("USER")

    assert(expectedAlias == alias)
  }

}

case class User(id: Int, name: String)
