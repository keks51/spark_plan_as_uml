package com.keks.plan.parser.narrow

import com.keks.plan.operations.SELECT
import com.keks.plan.parser.{DefaultExpressionParser, SparkLogicalRelationParser}
import org.apache.spark.sql.functions.{col, count}
import org.apache.spark.sql.types.IntegerType
import utils.TestBase

class SparkDfLogicalParserSelectTest extends TestBase {

  import spark.implicits._

  val planParser = new SparkLogicalRelationParser(new DefaultExpressionParser())


  /* Project [user_id#17, order_id#18, name#19]
     +- Project [_1#8 AS user_id#17, _2#9 AS order_id#18, _3#10 AS name#19, _4#11 AS age#20, _5#12 AS job#21, _6#13 AS a4#22, _7#14 AS a5#23, _8#15 AS a6#24]
        +- LocalRelation [_1#8, _2#9, _3#10, _4#11, _5#12, _6#13, _7#14, _8#15] */

  "project SelectTransformation" should "test1" in {
    val usersAnalyzed = Seq(("a", "1", "a", "a", "a", "a", "a", "a"))
      .toDF("user_id", "order_id", "name", "age", "job", "a4", "a5", "a6")
      .select("user_id", "order_id", "name")
      .queryExecution.analyzed

    val (res, _) = planParser.parse(usersAnalyzed, None)

    assert(res.transformationName == SELECT)
  }


  /* Project [user_id#17, order_id#33, name#19]
     +- Project [user_id#17, cast(order_id#18 as int) AS order_id#33, name#19, age#20, job#21, a4#22, a5#23, a6#24]
        +- Project [_1#8 AS user_id#17, _2#9 AS order_id#18, _3#10 AS name#19, _4#11 AS age#20, _5#12 AS job#21, _6#13 AS a4#22, _7#14 AS a5#23, _8#15 AS a6#24]
           +- LocalRelation [_1#8, _2#9, _3#10, _4#11, _5#12, _6#13, _7#14, _8#15] */

  "project SelectTransformation" should "test2" in {
    val usersAnalyzed = Seq(("a", "1", "a", "a", "a", "a", "a", "a"))
      .toDF("user_id", "order_id", "name", "age", "job", "a4", "a5", "a6")
      .withColumn("order_id", col("order_id").cast(IntegerType))
      .select("user_id", "order_id", "name")
      .queryExecution.analyzed

    val (res, _) = planParser.parse(usersAnalyzed, None)

    assert(res.transformationName == SELECT)
  }


  /* Project [user_id#17, order_id#18]
     +- Aggregate [user_id#17, order_id#18, name#19, age#20, job#21, a4#22], [user_id#17, order_id#18, name#19, age#20, job#21, a4#22, count(order_id#18) AS count(order_id)#43L]
        +- Project [_1#8 AS user_id#17, _2#9 AS order_id#18, _3#10 AS name#19, _4#11 AS age#20, _5#12 AS job#21, _6#13 AS a4#22, _7#14 AS a5#23, _8#15 AS a6#24]
           +- LocalRelation [_1#8, _2#9, _3#10, _4#11, _5#12, _6#13, _7#14, _8#15] */

  "project SelectTransformation" should "test3" in {
    val usersAnalyzed = Seq(("a", "1", "a", "a", "a", "a", "a", "a"))
      .toDF("user_id", "order_id", "name", "age", "job", "a4", "a5", "a6")
      .groupBy("user_id", "order_id", "name", "age", "job", "a4").agg(count("order_id"))
      .select("user_id", "order_id")
      .queryExecution.analyzed

    val (res, _) = planParser.parse(usersAnalyzed, None)

    assert(res.transformationName == SELECT)
  }


}
