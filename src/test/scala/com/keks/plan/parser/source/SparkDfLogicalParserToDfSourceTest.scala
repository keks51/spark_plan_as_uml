package com.keks.plan.parser.source

import com.keks.plan.operations.narrow.WithColumnsTransformation
import com.keks.plan.operations.sources.ToDfSource
import com.keks.plan.parser.{DefaultExpressionParser, SparkLogicalRelationParser}
import org.apache.spark.sql.catalyst.AliasIdentifier
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoders, Row}
import utils.TestBase

class SparkDfLogicalParserToDfSourceTest extends TestBase {

  import spark.implicits._

  val planParser = new SparkLogicalRelationParser(new DefaultExpressionParser())


  /* Project [_1#2 AS user_id#5, _2#3 AS order_id#6]
     +- LocalRelation [_1#2, _2#3] */
  "ToDfSource" should "by code created source" in {
    val usersAnalyzed = Seq(("a", "b"))
      .toDF("user_id", "order_id")
      .queryExecution
      .analyzed

    val (datasource, _) = planParser.parse(usersAnalyzed, None)
    assert(datasource.isInstanceOf[ToDfSource])
  }

  "ToDfSource" should "be the same generated source" in {
    val usersDF = Seq(("a", "b", "a", "a", "a", "a", "a", "a"))
      .toDF("user_id", "order_id", "name", "age", "job", "a4", "a5", "a6")

    val user1 = usersDF
    val user2 = usersDF
    val (datasource1, _) = planParser.parse(user1.queryExecution.analyzed, None)
    val (datasource2, _) = planParser.parse(user2.queryExecution.analyzed, None)
    assert(datasource1 == datasource2)
  }

  "ToDfSource" should "NOT be the same generated source" in {
    val usersDF1 = Seq(("a", "b", "a", "a", "a", "a", "a", "a"))
      .toDF("user_id", "order_id", "name", "age", "job", "a4", "a5", "a6")
    val usersDF2 = Seq(("a", "b", "a", "a", "a", "a", "a", "a"))
      .toDF("user_id", "order_id", "name", "age", "job", "a4", "a5", "a6")

    val user1 = usersDF1
    val user2 = usersDF2
    val (datasource1, _) = planParser.parse(user1.queryExecution.analyzed, None)
    val (datasource2, _) = planParser.parse(user2.queryExecution.analyzed, None)
    assert(datasource1 != datasource2)
  }


  /* Project [_1#5 AS user_id#11, _2#6 AS is_student#12, _3#7 AS age#13, _4#8 AS salary#14, _5#9L AS unique_id#15L]
     +- LocalRelation [_1#5, _2#6, _3#7, _4#8, _5#9L] */
  "ToDfSource" should "contain correct data types" in {
    val usersAnalyzed = Seq(("alex", true, 25, 400.0, 232L))
      .toDF("user_id", "is_student", "age", "salary", "unique_id")
      .queryExecution.analyzed

    val resColumns = planParser
      .parse(usersAnalyzed, None)
      ._1
      .asInstanceOf[ToDfSource]
      .sourceColumns
      .map(e => (e.name, e.dataType))

    val expectedColumns = Array(
      ("user_id", StringType),
      ("is_student", BooleanType),
      ("age", IntegerType),
      ("salary", DoubleType),
      ("unique_id", LongType)
    )
    expectedColumns should contain theSameElementsAs resColumns
  }


  /* SubqueryAlias `USER`
     +- Project [id#4]
        +- LogicalRDD [id#4, name#5], false */
  "ToDfSource" should "test4" in {
    val schema = Encoders.product[User].schema
    val usersAnalyzed = spark
      .createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
      .select("id")
      .as("USER")
      .queryExecution
      .analyzed


    val alias: AliasIdentifier = planParser
      .parse(usersAnalyzed, None)
      ._1
      .asInstanceOf[ToDfSource]
      .name
      .get

    val expectedAlias = AliasIdentifier("USER")

    assert(expectedAlias == alias)
  }


  /* SubqueryAlias `USER`
     +- Project [id#2]
        +- LocalRelation [id#2, name#3] */
  "ToDfSource" should "test5" in {
    val usersAnalyzed = spark.createDataset(Seq(User(1, "bob")))
      .select("id")
      .as("USER")
      .queryExecution
      .analyzed

    val alias: AliasIdentifier = planParser
      .parse(usersAnalyzed, None)
      ._1
      .asInstanceOf[ToDfSource]
      .name
      .get

    val expectedAlias = AliasIdentifier("USER")

    assert(expectedAlias == alias)
  }


  /* SubqueryAlias `USERS`
     +- Project [_1#2 AS id#5, _2#3 AS name#6]
        +- LocalRelation [_1#2, _2#3] */
  "ToDfSource" should "test6" in {
    val usersAnalyzed = Seq((1, "alex"), (3, "bob"))
      .toDF("id", "name")
      .as[User]
      .as("USERS")
      .queryExecution.analyzed

    val (transformation1, _) = planParser.parse(usersAnalyzed, None)

    assert(transformation1.isInstanceOf[ToDfSource])
  }


  /* SubqueryAlias `USER`
     +- Project [id#4]
        +- LogicalRDD [id#4, name#5], false */
  "CreateDataFrameSource" should "test7" in {
    val schema = Encoders.product[User].schema
    val usersAnalyzed = spark
      .createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
      .select("id")
      .as("USER")
      .queryExecution
      .analyzed

    val alias: AliasIdentifier = planParser
      .parse(usersAnalyzed, None)
      ._1
      .asInstanceOf[ToDfSource]
      .name
      .get

    val expectedAlias = AliasIdentifier("USER")

    assert(expectedAlias == alias)
  }



}
