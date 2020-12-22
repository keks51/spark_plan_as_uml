package com.keks.plan

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import utils.TestBase


class SparkPlanParserTest extends TestBase {

  import spark.implicits._

  "SparkUmlDiagram" should "build report" in {
    val users = Seq(("a", "b", "a", "a", "a", "a", "a", "a"))
      .toDF("user_id", "order_id", "a1", "a2", "a3", "a4", "a5", "a6")
      .as("USER")
      .withColumn("user_id", col("user_id").cast(IntegerType)).as("USER2")
      .groupBy("user_id", "order_id", "a1", "a2", "a3", "a4", "a5", "a6").agg(count("order_id"))
    val managers = Seq("a").toDF("manager_user_id").as("MANAGERS").cache
    val filteredManagers = managers.filter(col("manager_user_id") =!= "alex")
    val res = users.join(filteredManagers, col("user_id") =!= col("manager_user_id")).as[User].map(identity)
    val plan: LogicalPlan = res.queryExecution.logical

    SparkUmlDiagram.buildAndSaveReport(s"_super_new_orders2",
                                       "",
                                       plan,
                                       "example_reports")
  }

}

case class User(order_id: String,
                manager_user_id: String,
                user_id: String)
