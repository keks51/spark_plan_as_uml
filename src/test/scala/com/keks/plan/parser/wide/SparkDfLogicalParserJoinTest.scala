package com.keks.plan.parser.wide

import com.keks.plan.operations.JOIN
import com.keks.plan.parser.{DefaultExpressionParser, SparkLogicalRelationParser}
import utils.TestBase

class SparkDfLogicalParserJoinTest extends TestBase {

  import spark.implicits._

  val planParser = new SparkLogicalRelationParser(new DefaultExpressionParser())


  /* Project [id#5, name#6, name#15]
     +- Join Inner, (id#5 = id#14)
        :- Project [_1#2 AS id#5, _2#3 AS name#6]
        :  +- LocalRelation [_1#2, _2#3]
        +- Project [_1#11 AS id#14, _2#12 AS name#15]
           +- LocalRelation [_1#11, _2#12] */
  "Join" should "test1" in {
    val users = Seq((1, "alex"), (3, "bob")).toDF("id", "name")
    val jobs = Seq((1, "it"), (2, "dentist")).toDF("id", "name")

    val joined = users.join(jobs, Seq("id"))

    val joinedAnalyzed = joined.queryExecution.analyzed

    val (transformation1, _) = planParser.parse(joinedAnalyzed, None)

    assert(transformation1.transformationName == JOIN)
  }


  /* Project [id#5, name#6, job#15]
     +- Join Inner, (id#5 = id#14)
        :- Project [_1#2 AS id#5, _2#3 AS name#6]
        :  +- LocalRelation [_1#2, _2#3]
        +- Project [_1#11 AS id#14, _2#12 AS job#15]
           +- LocalRelation [_1#11, _2#12] */
  "Join" should "test2" in {
    val users = Seq((1, "alex"), (3, "bob")).toDF("id", "name")
    val jobs = Seq((1, "it"), (2, "dentist")).toDF("id", "job")

    val joined = users.join(jobs, Seq("id"))

    val joinedAnalyzed = joined.queryExecution.analyzed

    val (transformation1, _) = planParser.parse(joinedAnalyzed, None)

    assert(transformation1.transformationName == JOIN)
  }

}
