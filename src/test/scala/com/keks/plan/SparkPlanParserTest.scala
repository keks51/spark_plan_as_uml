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
                                       "examples")
  }

  "SparkUmlDiagram" should "build room example" in {
    val userDF = Seq(("a", "b")).toDF("user_id", "user_name").as("USER_TABLE")
    val phoneDF = Seq(("a", "b", "c")).toDF("phone_id", "user_id", "phone_number").as("PHONE_TABLE")
    val roomDF = Seq(("a", "b", "c")).toDF("room_id", "phone_id", "room_number").as("ROOM_TABLE")

    // find all rooms where 'alex' users live with phone_number starting with '+7952'
    val alexUsers = userDF.filter(lower(col("user_name")) === "alex")
    val filteredPhones = phoneDF.filter(col("phone_number").startsWith("+7952"))

    val plan = alexUsers
      .join(filteredPhones, Seq("user_id"), "inner")
      .join(roomDF, Seq("phone_id"))
      .select("room_id", "room_number")

    SparkUmlDiagram.buildAndSaveReport(s"rooms",
                                       "",
                                       plan.queryExecution.logical,
                                       "examples")
  }

}

case class User(order_id: String,
                manager_user_id: String,
                user_id: String)
