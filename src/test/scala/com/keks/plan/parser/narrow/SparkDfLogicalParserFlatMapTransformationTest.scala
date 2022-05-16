package com.keks.plan.parser.narrow

import com.keks.plan.operations.narrow.FlatMapTransformation
import com.keks.plan.parser.narrow.SparkDfLogicalParserFlatMapTransformationTest._
import com.keks.plan.parser.source.User
import com.keks.plan.parser.{DefaultExpressionParser, SparkLogicalRelationParser}
import org.apache.spark.sql.types.{DataType, IntegerType, ObjectType, StringType}
import utils.TestBase


class SparkDfLogicalParserFlatMapTransformationTest extends TestBase {

  import spark.implicits._

  val planParser = new SparkLogicalRelationParser(new DefaultExpressionParser())


  /* SerializeFromObject [assertnotnull(assertnotnull(input[0, com.keks.plan.parser.narrow.StudentFlatMap, true])).id AS id#17, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, assertnotnull(assertnotnull(input[0, com.keks.plan.parser.narrow.StudentFlatMap, true])).name, true, false) AS name#18, assertnotnull(assertnotnull(input[0, com.keks.plan.parser.narrow.StudentFlatMap, true])).age AS age#19]
     +- MapPartitions <function1>, obj#16: com.keks.plan.parser.narrow.StudentFlatMap
        +- DeserializeToObject newInstance(class com.keks.plan.parser.source.User), obj#15: com.keks.plan.parser.source.User
           +- Project [_1#2 AS id#5, _2#3 AS name#6]
              +- LocalRelation [_1#2, _2#3] */
  "FlatMapTransformation" should "test1" in {
    val users = Seq((1, "alex"), (3, "bob"))
      .toDF("id", "name")
      .as[User]
      .flatMap(fooBar1)

    val usersAnalyzed = users.queryExecution.analyzed
    val (transformation1, _) = planParser.parse(usersAnalyzed, None)

    assert(transformation1.isInstanceOf[FlatMapTransformation])

    val resSchema: Seq[(String, DataType)] = transformation1.asInstanceOf[FlatMapTransformation].schema
    val expSchema: Seq[(String, DataType)] = Seq(
      ("id", IntegerType),
      ("name", StringType),
      ("age", IntegerType)
    )

    expSchema should contain theSameElementsAs resSchema
  }


  /* SerializeFromObject [assertnotnull(assertnotnull(input[0, scala.Tuple2, true]))._1 AS _1#16, assertnotnull(assertnotnull(input[0, scala.Tuple2, true]))._2 AS _2#17]
     +- MapPartitions <function1>, obj#15: scala.Tuple2
        +- DeserializeToObject newInstance(class com.keks.plan.parser.source.User), obj#14: com.keks.plan.parser.source.User
           +- Project [_1#2 AS id#5, _2#3 AS name#6]
              +- LocalRelation [_1#2, _2#3] */
  "FlatMapTransformation" should "test2" in {
    val usersAnalyzed = Seq((1, "alex"), (3, "bob"))
      .toDF("id", "name")
      .as[User]
      .flatMap(fooBar2)
      .queryExecution.analyzed

    val (transformation1, _) = planParser.parse(usersAnalyzed, None)

    assert(transformation1.isInstanceOf[FlatMapTransformation])

    val resSchema: Seq[(String, DataType)] = transformation1.asInstanceOf[FlatMapTransformation].schema
    assert(resSchema.size == 1)
    assert(resSchema.head._2.isInstanceOf[ObjectType])
  }

}

object SparkDfLogicalParserFlatMapTransformationTest {

  def fooBar1(user: User): Array[StudentFlatMap] = {
    Array(StudentFlatMap(user.id, user.name, 15))
  }

  def fooBar2(user: User): Array[(Double, Boolean)] = {
    Array((1.0, false))
  }

}

case class StudentFlatMap(id: Int, name: String, age: Int)

case class CitySalaryFlatMap(city: String, salary: Double)