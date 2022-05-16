package com.keks.plan.parser.narrow

import com.keks.plan.operations.MAP_TRANSFORMATION
import com.keks.plan.operations.narrow.MapTransformation
import com.keks.plan.parser.narrow.SparkDfLogicalParserMapTransformationTestUtils.{fooBar1, fooBar2}
import com.keks.plan.parser.source.User
import com.keks.plan.parser.{DefaultExpressionParser, SparkLogicalRelationParser}
import org.apache.spark.sql.types.{DataType, IntegerType, ObjectType, StringType}
import utils.TestBase


class SparkDfLogicalParserMapTransformationTest extends TestBase {

  import spark.implicits._

  val planParser = new SparkLogicalRelationParser(new DefaultExpressionParser())

  /* SerializeFromObject [staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, assertnotnull(assertnotnull(input[0, com.keks.plan.parser.narrow.CitySalaryMap, true])).city, true, false) AS city#16, assertnotnull(assertnotnull(input[0, com.keks.plan.parser.narrow.CitySalaryMap, true])).salary AS salary#17]
     +- MapElements <function1>, class com.keks.plan.parser.narrow.CitySalaryMap, [StructField(city,StringType,true), StructField(salary,DoubleType,false)], obj#15: com.keks.plan.parser.narrow.CitySalaryMap
        +- DeserializeToObject newInstance(class com.keks.plan.parser.narrow.CitySalaryMap), obj#14: com.keks.plan.parser.narrow.CitySalaryMap
           +- Project [_1#2 AS city#5, _2#3 AS salary#6]
              +- LocalRelation [_1#2, _2#3] */
  "MapTransformation" should "test1" in {
    val cityAndSalaryAnalyzed = Seq(("moscow", 10.0))
      .toDF("city", "salary")
      .as[CitySalaryMap]
      .map(identity)
      .queryExecution.analyzed

    val (transformation1, _) = planParser.parse(cityAndSalaryAnalyzed, None)

    assert(transformation1.transformationName == MAP_TRANSFORMATION)
  }


  /* SerializeFromObject [staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, assertnotnull(assertnotnull(input[0, com.keks.plan.parser.narrow.CitySalaryMap, true])).city, true, false) AS city#18, assertnotnull(assertnotnull(input[0, com.keks.plan.parser.narrow.CitySalaryMap, true])).salary AS salary#19]
     +- MapElements <function1>, class com.keks.plan.parser.narrow.CitySalaryMap, [StructField(city,StringType,true), StructField(salary,DoubleType,false)], obj#17: com.keks.plan.parser.narrow.CitySalaryMap
        +- DeserializeToObject newInstance(class com.keks.plan.parser.narrow.CitySalaryMap), obj#16: com.keks.plan.parser.narrow.CitySalaryMap
           +- Project [city#5, salary#6]
              +- Project [_1#2 AS city#5, _2#3 AS salary#6]
                 +- LocalRelation [_1#2, _2#3] */
  "MapTransformation" should "test2" in {
    val cityAndSalaryAnalyzed = Seq(("moscow", 10.0))
      .toDF("city", "salary")
      .select("city", "salary")
      .as[CitySalaryMap]
      .map(identity)
      .queryExecution.analyzed

    val (transformation1, _) = planParser.parse(cityAndSalaryAnalyzed, None)

    assert(transformation1.transformationName == MAP_TRANSFORMATION)
  }


  /* SerializeFromObject [assertnotnull(assertnotnull(input[0, com.keks.plan.parser.narrow.StudentMap, true])).id AS id#17, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, assertnotnull(assertnotnull(input[0, com.keks.plan.parser.narrow.StudentMap, true])).name, true, false) AS name#18, assertnotnull(assertnotnull(input[0, com.keks.plan.parser.narrow.StudentMap, true])).age AS age#19]
     +- MapElements <function1>, class com.keks.plan.parser.source.User, [StructField(id,IntegerType,false), StructField(name,StringType,true)], obj#16: com.keks.plan.parser.narrow.StudentMap
        +- DeserializeToObject newInstance(class com.keks.plan.parser.source.User), obj#15: com.keks.plan.parser.source.User
           +- Project [_1#2 AS id#5, _2#3 AS name#6]
              +- LocalRelation [_1#2, _2#3] */
  "MapTransformation" should "test3" in {
    val users = Seq((1, "alex"), (3, "bob"))
      .toDF("id", "name")
      .as[User]
      .map(fooBar1)

    val usersAnalyzed = users.queryExecution.analyzed
    val (transformation1, _) = planParser.parse(usersAnalyzed, None)

    assert(transformation1.isInstanceOf[MapTransformation])

    val resSchema: Seq[(String, DataType)] = transformation1.asInstanceOf[MapTransformation].schema
    val expSchema: Seq[(String, DataType)] = Seq(
      ("id", IntegerType),
      ("name", StringType),
      ("age", IntegerType)
    )

    expSchema should contain theSameElementsAs resSchema
  }


  /* SerializeFromObject [assertnotnull(assertnotnull(input[0, scala.Tuple2, true]))._1 AS _1#16, assertnotnull(assertnotnull(input[0, scala.Tuple2, true]))._2 AS _2#17]
     +- MapElements <function1>, class com.keks.plan.parser.source.User, [StructField(id,IntegerType,false), StructField(name,StringType,true)], obj#15: scala.Tuple2
        +- DeserializeToObject newInstance(class com.keks.plan.parser.source.User), obj#14: com.keks.plan.parser.source.User
           +- Project [_1#2 AS id#5, _2#3 AS name#6]
              +- LocalRelation [_1#2, _2#3] */
  "MapTransformation" should "test4" in {
    val usersAnalyzed = Seq((1, "alex"), (3, "bob"))
      .toDF("id", "name")
      .as[User]
      .map(fooBar2)
      .queryExecution.analyzed
    val (transformation1, _) = planParser.parse(usersAnalyzed, None)

    assert(transformation1.isInstanceOf[MapTransformation])

    val resSchema: Seq[(String, DataType)] = transformation1.asInstanceOf[MapTransformation].schema
    assert(resSchema.size == 1)
    assert(resSchema.head._2.isInstanceOf[ObjectType])
  }

}

object SparkDfLogicalParserMapTransformationTestUtils {

  def fooBar1(user: User): StudentMap = {
    StudentMap(1, "", 10)
  }

  def fooBar2(user: User): (Double, Boolean) = {
    (1.0, false)
  }
}

case class StudentMap(id: Int, name: String, age: Int)

case class CitySalaryMap(city: String, salary: Double)