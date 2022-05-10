package com.keks.plan.parser.source.hadoop

import com.keks.plan.operations.sources.HadoopSource
import com.keks.plan.parser.{DefaultExpressionParser, SparkLogicalRelationParser}
import org.apache.spark.sql.types.{BooleanType, DoubleType, LongType, StringType}
import utils.TestBase

class SparkDfLogicalParserJsonSourceTest extends TestBase {

  import spark.implicits._
  val planParser = new SparkLogicalRelationParser(new DefaultExpressionParser())


/* Relation[age#32L,is_student#33,salary#34,unique_id#35L,user_id#36] json */
  "JsonSource" should "by code created source" in withTempDir { dir =>
    val usersDF = Seq(("alex", true, 25, 400.0, 232L))
      .toDF("user_id", "is_student", "age", "salary", "unique_id")
    val path = s"$dir/users"
    usersDF.repartition(1).write.json(path)

    val usersJsonAnalyzed = spark.read.json(path).queryExecution.analyzed

    val (datasource, _) = planParser.parse(usersJsonAnalyzed, None)
    assert(datasource.transformationName ==  s"JSON_FILE_SOURCE_TABLE")
  }



  "JsonSource" should "be the same generated source" in withTempDir { dir =>
    val usersDF = Seq(("a", "b", "a", "a", "a", "a", "a"))
      .toDF("user_id", "order_id", "name", "age", "job", "company", "sex")
    val path = s"$dir/users"
    usersDF.repartition(1).write.json(path)
    val usersDF1 = spark.read.json(path)
    val usersDF2 = spark.read.json(path)

    val (datasource1, _) = planParser.parse(usersDF1.queryExecution.analyzed, None)
    val (datasource2, _) = planParser.parse(usersDF2.queryExecution.analyzed, None)

    assert(datasource1 == datasource2)
  }



  "JsonSource" should "NOT be the same generated source" in withTempDir { dir =>
    val usersDF = Seq(("a", "b", "a", "a", "a", "a", "a"))
      .toDF("user_id", "order_id", "name", "age", "job", "company", "sex")
    val path1 = s"$dir/users1"
    usersDF.repartition(1).write.json(path1)
    val path2 = s"$dir/users2"
    usersDF.repartition(1).write.json(path2)
    val usersDF1 = spark.read.json(path1)
    val usersDF2 = spark.read.json(path2)

    val (datasource1, _) = planParser.parse(usersDF1.queryExecution.analyzed, None)
    val (datasource2, _) = planParser.parse(usersDF2.queryExecution.analyzed, None)

    assert(datasource1 != datasource2)
  }


/* Relation[age#32L,is_student#33,salary#34,unique_id#35L,user_id#36] json */
  "JsonSource" should "contain correct data types" in withTempDir { dir =>
    val usersDF = Seq(("alex", true, 25, 400.0, 232L))
      .toDF("user_id", "is_student", "age", "salary", "unique_id")
    val path = s"$dir/users"
    usersDF.repartition(1).write.json(path)

    val usersJsonAnalyzed = spark.read.json(path).queryExecution.analyzed

    val resColumns = planParser
      .parse(usersJsonAnalyzed, None)
      ._1
      .asInstanceOf[HadoopSource]
      .sourceColumns
      .map(e => (e.name, e.dataType))

    val expectedColumns = Array(
      ("user_id", StringType),
      ("is_student", BooleanType),
      ("age", LongType),
      ("salary", DoubleType),
      ("unique_id", LongType)
    )
    expectedColumns should contain theSameElementsAs resColumns
  }


}
