package com.keks.plan.parser.narrow

import com.keks.plan.operations.narrow.AliasTransformation
import com.keks.plan.operations.{CREATED_BY_CODE, TABLE_ALIAS}
import com.keks.plan.parser.source.User
import com.keks.plan.parser.{DefaultExpressionParser, SparkLogicalRelationParser}
import org.apache.spark.sql.{Encoders, Row}
import org.apache.spark.sql.functions.col
import utils.TestBase

class SparkDfLogicalParserSubqueryAliasTest extends TestBase {

  import spark.implicits._

  val planParser = new SparkLogicalRelationParser(new DefaultExpressionParser())


  /* SubqueryAlias `USERS`
     +- Project [_1#2 AS id#5, _2#3 AS name#6]
        +- LocalRelation [_1#2, _2#3] */
  "SubdueAlias" should "test1" in {
    val usersAnalyzed = Seq((1, "alex"), (3, "bob"))
      .toDF("id", "name")
      .as("USERS")
      .queryExecution.analyzed

    val (transformation1, _) = planParser.parse(usersAnalyzed, None)

    assert(transformation1.transformationName == CREATED_BY_CODE)
  }


  /* SubqueryAlias `USERS`
     +- Project [id#5]
        +- Project [_1#2 AS id#5, _2#3 AS name#6]
           +- LocalRelation [_1#2, _2#3] */
  "SubdueAlias" should "test2" in {
    val usersAnalyzed = Seq((1, "alex"), (3, "bob"))
      .toDF("id", "name")
      .select("id")
      .as("USERS")
      .queryExecution.analyzed

    val (transformation1, _) = planParser.parse(usersAnalyzed, None)

    assert(transformation1.transformationName == TABLE_ALIAS)
  }


  /* SubqueryAlias `USERS`
     +- Project [id#5]
        +- Project [id#5]
           +- Project [_1#2 AS id#5, _2#3 AS name#6]
              +- LocalRelation [_1#2, _2#3] */
  "SubdueAlias" should "test3" in {
    val usersAnalyzed = Seq((1, "alex"), (3, "bob"))
      .toDF("id", "name")
      .select("id")
      .select("id")
      .as("USERS")
      .queryExecution.analyzed

    val (transformation1, _) = planParser.parse(usersAnalyzed, None)

    assert(transformation1.transformationName == TABLE_ALIAS)
  }


  /* SubqueryAlias `USERS`
     +- Project [id#5, (cast(name#6 as double) + cast(id#5 as double)) AS (name + id)#9]
        +- Project [_1#2 AS id#5, _2#3 AS name#6]
           +- LocalRelation [_1#2, _2#3] */
  "SubdueAlias" should "test4" in {
    val usersAnalyzed = Seq((1, "alex"), (3, "bob"))
      .toDF("id", "name")
      .select(col("id"), col("name") + col("id"))
      .as("USERS")
      .queryExecution.analyzed

    val (transformation1, _) = planParser.parse(usersAnalyzed, None)

    assert(transformation1.isInstanceOf[AliasTransformation])
  }


  /* SubqueryAlias `USER`
     +- Project [id#4, (cast(id#4 as double) + cast(name#5 as double)) AS (id + name)#8]
        +- LogicalRDD [id#4, name#5], false */
  "SubdueAlias" should "test5" in {
    val schema = Encoders.product[User].schema
    val usersAnalyzed = spark
      .createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
      .select(col("id"), col("id") + col("name"))
      .as("USER")
      .queryExecution
      .analyzed

    val transformation = planParser
      .parse(usersAnalyzed, None)
      ._1

    assert(transformation.isInstanceOf[AliasTransformation])
  }

}
