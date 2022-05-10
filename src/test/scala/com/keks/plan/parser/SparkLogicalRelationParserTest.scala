package com.keks.plan.parser

import com.keks.plan.operations.sources.utils.SourceFilesInfo
import com.keks.plan.operations.{COLUMNS_MODIFICATIONS, COLUMNS_OPERATION, CREATED_BY_CODE, SELECT, SKIP_OPERATION, TransformationLogicTrait, WINDOW}
import com.keks.plan.parser.source.User
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.plans.logical.MapElements
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions.{avg, col, count, explode, udf}
import org.apache.spark.sql.types.IntegerType
import utils.TestBase

import scala.reflect.runtime.currentMirror


class SparkLogicalRelationParserTest extends TestBase {

  import spark.implicits._
  val planParser = new SparkLogicalRelationParser(new DefaultExpressionParser())







//  "ddfd" should "fdfda" in {
//    val users1 = Seq(("a", "1", "a", "a", "a", "a", "a", "a"))
//      .toDF("user_id", "order_id", "a1", "a2", "a3", "a4", "a5", "a6")
//      .as("USER")
//      .withColumn("user_id", col("user_id").cast(IntegerType))
//      .select("user_id", "order_id", "a1", "a2", "a3", "a4")
//    val anal1 = users1.queryExecution.analyzed
//
//    val users2 = Seq(("a", "1", "a", "a", "a", "a", "a", "a"))
//      .toDF("user_id", "order_id", "a1", "a2", "a3", "a4", "a5", "a6")
//      .as("USER")
//      .select(col("user_id"), col("order_id").cast(IntegerType), col("a1"), col("a2"), col("a3"), col("a4"))
//    val anal2 = users2.queryExecution.analyzed
//
//    println("fdfd")
//  }

//  "proje" should "be the same generated source" in {
//
//    val cityAndSalary = Seq(("moscow", 10.0)).toDF("city", "salary")
//
//    val avg1 = cityAndSalary
//      .groupBy("city").agg(avg("salary").as("avg_salary")).queryExecution.analyzed
//
//    val avg2 = cityAndSalary
//      .withColumn("avg_salary", avg("salary").over(Window.partitionBy("city"))).queryExecution.analyzed
//
//    val avg3 = cityAndSalary
//      .select(col("city"), avg("salary").over(Window.partitionBy("city"))).queryExecution.analyzed
//
////    val datasource1 = planParser.parse(user1.queryExecution.analyzed, None)
//
//    println("fd")
//  }
}

