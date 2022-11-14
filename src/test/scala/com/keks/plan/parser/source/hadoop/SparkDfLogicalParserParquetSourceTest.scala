package com.keks.plan.parser.source.hadoop

import com.keks.plan.operations.narrow.WithColumnsTransformation
import com.keks.plan.operations.sources.HadoopSource
import com.keks.plan.parser.{DefaultExpressionParser, SparkLogicalRelationParser}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import utils.TestBase


class SparkDfLogicalParserParquetSourceTest extends TestBase {

  import spark.implicits._
  val planParser = new SparkLogicalRelationParser(new DefaultExpressionParser())


/* Relation[user_id#26,is_student#27,age#28,salary#29,unique_id#30L] parquet */
  "ParquetSource" should "by code created source" in withTempDir { dir =>
    val usersDF = Seq(("alex", true, 25, 400.0, 232L))
      .toDF("user_id", "is_student", "age", "salary", "unique_id")
    val path = s"$dir/users"
    usersDF.repartition(1).write.parquet(path)

    val usersJsonAnalyzed = spark.read.parquet(path).queryExecution.analyzed

    val (datasource, _) = planParser.parse(usersJsonAnalyzed, None)
    assert(datasource.transformationName ==  s"PARQUET_FILE_SOURCE_TABLE")
  }

  "ParquetSource" should "be the same generated source" in withTempDir { dir =>
    val usersDF = Seq(("a", "b", "a", "a", "a", "a", "a"))
      .toDF("user_id", "order_id", "name", "age", "job", "company", "sex")
    val path = s"$dir/users"
    usersDF.repartition(1).write.parquet(path)
    val usersDF1 = spark.read.parquet(path)
    val usersDF2 = spark.read.parquet(path)

    val (datasource1, _) = planParser.parse(usersDF1.queryExecution.analyzed, None)
    val (datasource2, _) = planParser.parse(usersDF2.queryExecution.analyzed, None)

    assert(datasource1 == datasource2)
  }

  "ParquetSource" should "NOT be the same generated source" in withTempDir { dir =>
    val usersDF = Seq(("a", "b", "a", "a", "a", "a", "a"))
      .toDF("user_id", "order_id", "name", "age", "job", "company", "sex")
    val path1 = s"$dir/users1"
    usersDF.repartition(1).write.parquet(path1)
    val path2 = s"$dir/users2"
    usersDF.repartition(1).write.parquet(path2)
    val usersDF1 = spark.read.parquet(path1)
    val usersDF2 = spark.read.parquet(path2)

    val (datasource1, _) = planParser.parse(usersDF1.queryExecution.analyzed, None)
    val (datasource2, _) = planParser.parse(usersDF2.queryExecution.analyzed, None)

    assert(datasource1 != datasource2)
  }


/* Relation[user_id#26,is_student#27,age#28,salary#29,unique_id#30L] parquet */
  "ParquetSource" should "contain correct data types" in withTempDir { dir =>
    val usersDF = Seq(("alex", true, 25, 400.0, 232L))
      .toDF("user_id", "is_student", "age", "salary", "unique_id")
    val path = s"$dir/users"
    usersDF.repartition(1).write.parquet(path)

    val usersJsonAnalyzed = spark.read.parquet(path).queryExecution.analyzed

    val resColumns = planParser
      .parse(usersJsonAnalyzed, None)
      ._1
      .asInstanceOf[HadoopSource]
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


  /* Project [user_id#26, is_student#27]
     +- Relation[user_id#26,is_student#27,age#28,salary#29,unique_id#30L] parquet */
  "ParquetSource" should "test3" in withTempDir { dir =>
    val usersDF = Seq(("alex", true, 25, 400.0, 232L))
      .toDF("user_id", "is_student", "age", "salary", "unique_id")
    val path = s"$dir/users"
    usersDF.repartition(1).write.parquet(path)

    val usersJsonAnalyzed = spark
      .read
      .parquet(path)
      .select("user_id", "is_student")
      .queryExecution.analyzed

    val (res, _) = planParser
      .parse(usersJsonAnalyzed, None)
    assert(res.transformationName ==  s"PARQUET_FILE_SOURCE_TABLE")
  }


  /* Project [user_id#26, cast(is_student#27 as string) AS is_student#36]
     +- Relation[user_id#26,is_student#27,age#28,salary#29,unique_id#30L] parquet */
  "ParquetSource" should "test4" in withTempDir { dir =>
    val usersDF = Seq(("alex", true, 25, 400.0, 232L))
      .toDF("user_id", "is_student", "age", "salary", "unique_id")
    val path = s"$dir/users"
    usersDF.repartition(1).write.parquet(path)

    val usersJsonAnalyzed = spark
      .read
      .parquet(path)
      .select(col("user_id"), col("is_student").cast(StringType))
      .queryExecution.analyzed

    val (res, _) = planParser
      .parse(usersJsonAnalyzed, None)
    assert(res.isInstanceOf[WithColumnsTransformation])
  }


  /* SubqueryAlias `USER`
     +- Project [user_id#26, is_student#27]
        +- Relation[user_id#26,is_student#27,age#28,salary#29,unique_id#30L] parquet */
  "ParquetSource" should "test5" in withTempDir { dir =>
    val usersDF = Seq(("alex", true, 25, 400.0, 232L))
      .toDF("user_id", "is_student", "age", "salary", "unique_id")
    val path = s"$dir/users"
    usersDF.repartition(1).write.parquet(path)

    val usersJsonAnalyzed = spark
      .read
      .parquet(path)
      .select("user_id", "is_student")
      .as("USER")
      .queryExecution.analyzed

    val (res, _) = planParser
      .parse(usersJsonAnalyzed, None)
    assert(res.transformationName ==  s"PARQUET_FILE_SOURCE_TABLE")
    assert(res.asInstanceOf[HadoopSource].name.get.name == "USER")
  }


  /* Project [user_id#26, (cast(age#28 as double) * salary#29) AS (age * salary)#36]
     +- Relation[user_id#26,is_student#27,age#28,salary#29,unique_id#30L] parquet */
  "ParquetSource" should "test6" in withTempDir { dir =>
    val usersDF = Seq(("alex", true, 25, 400.0, 232L))
      .toDF("user_id", "is_student", "age", "salary", "unique_id")
    val path = s"$dir/users"
    usersDF.repartition(1).write.parquet(path)

    val usersJsonAnalyzed = spark
      .read
      .parquet(path)
      .select(col("user_id"), col("age") * col("salary"))
      .queryExecution.analyzed

    val (res, _) = planParser
      .parse(usersJsonAnalyzed, None)
    assert(res.isInstanceOf[WithColumnsTransformation])
  }

}
