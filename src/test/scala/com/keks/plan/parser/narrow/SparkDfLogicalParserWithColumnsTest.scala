package com.keks.plan.parser.narrow

import com.keks.plan.operations.COLUMNS_MODIFICATIONS
import com.keks.plan.operations.narrow.WithColumnsTransformation
import com.keks.plan.parser.narrow.SparkDfLogicalParserWithColumnsTest.concatUdf
import com.keks.plan.parser.{DefaultExpressionParser, SparkLogicalRelationParser}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.IntegerType
import utils.TestBase

class SparkDfLogicalParserWithColumnsTest extends TestBase {

  import spark.implicits._

  val planParser = new SparkLogicalRelationParser(new DefaultExpressionParser())


  /* Project [user_id#17, cast(order_id#18 as int) AS order_id#35, (cast(order_id#18 as int) + cast(order_id#18 as int)) AS (CAST(order_id AS INT) + CAST(order_id AS INT))#34, cast(order_id#18 as int) AS dead#33]
     +- Project [_1#8 AS user_id#17, _2#9 AS order_id#18, _3#10 AS name#19, _4#11 AS age#20, _5#12 AS job#21, _6#13 AS a4#22, _7#14 AS a5#23, _8#15 AS a6#24]
        +- LocalRelation [_1#8, _2#9, _3#10, _4#11, _5#12, _6#13, _7#14, _8#15] */
  "WithColumnsTransformation" should "test1" in {
    val usersAnalyzed = Seq(("a", "1", "a", "a", "a", "a", "a", "a"))
      .toDF("user_id", "order_id", "name", "age", "job", "a4", "a5", "a6")
      .select(
        col("user_id"),
        col("order_id").cast(IntegerType),
        col("order_id").cast(IntegerType) + col("order_id").cast(IntegerType),
        col("order_id").cast(IntegerType).as("dead")
      ).queryExecution.analyzed
    val (res, _) = planParser.parse(usersAnalyzed, None)

    assert(res.transformationName == COLUMNS_MODIFICATIONS)
  }


  /* Project [user_id#17, cast(order_id#18 as int) AS order_id#33, name#19, age#20, job#21, a4#22, a5#23, a6#24]
     +- Project [_1#8 AS user_id#17, _2#9 AS order_id#18, _3#10 AS name#19, _4#11 AS age#20, _5#12 AS job#21, _6#13 AS a4#22, _7#14 AS a5#23, _8#15 AS a6#24]
        +- LocalRelation [_1#8, _2#9, _3#10, _4#11, _5#12, _6#13, _7#14, _8#15] */
  "WithColumnsTransformation" should "test2" in {
    val usersAnalyzed = Seq(("a", "1", "a", "a", "a", "a", "a", "a"))
      .toDF("user_id", "order_id", "name", "age", "job", "a4", "a5", "a6")
      .withColumn("order_id", col("order_id").cast(IntegerType)).queryExecution.analyzed
    val (res, _) = planParser.parse(usersAnalyzed, None)

    assert(res.transformationName == COLUMNS_MODIFICATIONS)
  }


  /* Project [user_id#17, order_id#18, name#19, age#20, job#21, a4#22, a5#23, a6#24, cast(order_id#18 as int) AS order_id#33]
     +- Project [_1#8 AS user_id#17, _2#9 AS order_id#18, _3#10 AS name#19, _4#11 AS age#20, _5#12 AS job#21, _6#13 AS a4#22, _7#14 AS a5#23, _8#15 AS a6#24]
        +- LocalRelation [_1#8, _2#9, _3#10, _4#11, _5#12, _6#13, _7#14, _8#15] */
  "WithColumnsTransformation" should "test3" in {
    val usersAnalyzed = Seq(("a", "1", "a", "a", "a", "a", "a", "a"))
      .toDF("user_id", "order_id", "name", "age", "job", "a4", "a5", "a6")
      .select(col("*"), col("order_id").cast(IntegerType)).queryExecution.analyzed
    val (res, _) = planParser.parse(usersAnalyzed, None)

    assert(res.transformationName == COLUMNS_MODIFICATIONS)
  }


  /* Project [id#5, names#6, UDF(names#6) AS name#9]
     +- Project [_1#2 AS id#5, _2#3 AS names#6]
        +- LocalRelation [_1#2, _2#3] */
  "WithColumnsTransformation" should "test4" in {
    val usersAnalyzed = Seq((1, Array("alex", "bob")))
      .toDF("id", "names")
      .withColumn("name", concatUdf(col("names")))
      .queryExecution.analyzed

    val (transformation1, _) = planParser.parse(usersAnalyzed, None)

    assert(transformation1.transformationName == COLUMNS_MODIFICATIONS)
  }


  /* Project [id#5, (cast(name#6 as double) + cast(id#5 as double)) AS (name + id)#9]
     +- Project [_1#2 AS id#5, _2#3 AS name#6]
        +- LocalRelation [_1#2, _2#3] */
  "WithColumnsTransformation" should "test5" in {
    val usersAnalyzed = Seq((1, "alex"), (3, "bob"))
      .toDF("id", "name")
      .select(col("id"), col("name") + col("id"))
      .queryExecution.analyzed

    val (transformation1, _) = planParser.parse(usersAnalyzed, None)

    assert(transformation1.isInstanceOf[WithColumnsTransformation])
  }

}

object SparkDfLogicalParserWithColumnsTest {

  val concatUdf: UserDefinedFunction = udf((col: Array[String]) => col.mkString(" "))

}

