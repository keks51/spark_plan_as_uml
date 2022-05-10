package com.keks.plan.parser.narrow

import com.keks.plan.operations.UNION
import com.keks.plan.parser.{DefaultExpressionParser, SparkLogicalRelationParser}
import utils.TestBase

class SparkDfLogicalParserUnionTest extends TestBase {

  import spark.implicits._

  val planParser = new SparkLogicalRelationParser(new DefaultExpressionParser())


  /* Union
     :- Project [_1#2 AS id#5, _2#3 AS name#6]
     :  +- LocalRelation [_1#2, _2#3]
     +- Project [_1#11 AS id#14, _2#12 AS name#15]
        +- LocalRelation [_1#11, _2#12] */
  "Union" should "test1" in {
    val users = Seq((1, "alex"), (3, "bob")).toDF("id", "name")
    val jobs = Seq((1, "it"), (2, "dentist")).toDF("id", "name")

    val unioned = users.union(jobs)

    val joinedAnalyzed = unioned.queryExecution.analyzed

    val (transformation1, _) = planParser.parse(joinedAnalyzed, None)

    assert(transformation1.transformationName == UNION)
  }


  /* Union
     :- Project [_1#2 AS id#5, _2#3 AS name#6]
     :  +- LocalRelation [_1#2, _2#3]
     +- Project [_1#11 AS id#14, _2#12 AS name#15]
        +- LocalRelation [_1#11, _2#12] */
  "Union" should "test2" in {
    val users = Seq((1, "alex"), (3, "bob")).toDF("id", "name")
    val jobs = Seq((1, "it"), (2, "dentist")).toDF("id", "name")

    val unioned = users.unionAll(jobs)

    val joinedAnalyzed = unioned.queryExecution.analyzed

    val (transformation1, _) = planParser.parse(joinedAnalyzed, None)

    assert(transformation1.transformationName == UNION)
  }

}
