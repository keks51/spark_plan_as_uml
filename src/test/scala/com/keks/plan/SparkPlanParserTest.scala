package com.keks.plan

import com.keks.plan.builder.PlanUmlDiagramBuilder
import com.keks.plan.implicits._
import com.keks.plan.parser.{DefaultExpressionParser, SparkLogicalRelationParser}
import com.keks.plan.write.UmlPlanSaver
import org.apache.spark.sql.{Encoders, Row}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, With}
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import utils.TestBase


class SparkPlanParserTest extends TestBase {

  import spark.implicits._

  "SparkUmlDiagram" should "build report" in {
    val users = Seq(("a", "b", "a", "a", "a", "a", "a", "a"))
      .toDF("user_id", "order_id", "a1", "a2", "a3", "a4", "a5", "a6")
      .as("USER")
      .withColumn("user_id", col("user_id").cast(IntegerType))
      .as("USER2")
      .select("user_id", "order_id", "a1", "a2", "a3", "a4")
      .groupBy("user_id", "order_id", "a1", "a2", "a3", "a4").agg(count("order_id"))
    val managers = Seq("a").toDF("manager_user_id").as("MANAGERS").cache
    val filteredManagers = managers.filter(col("manager_user_id") =!= "alex")
    val resDF = users
      .join(filteredManagers, col("user_id") =!= col("manager_user_id"))
      .as[User]
      .map(identity)
    val l = resDF.queryExecution.logical
    val logicNodes = parse(resDF.queryExecution.logical)
    val analyzedNodes = parse(resDF.queryExecution.analyzed)
    resDF.printPlan(planParser = new SparkLogicalRelationParser(new DefaultExpressionParser()),
                 builder = new PlanUmlDiagramBuilder(),
                 entityName = s"_super_new_orders2",
                 reportDescription = "",
                 savePath = "examples",
                 saver = new UmlPlanSaver())

  }

  "SparkUmlDiagram" should "build report2" in {
    val schema = Encoders.product[User].schema
    val resDF = spark
      .createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
      .filter(col("order_id") === "123")

    resDF.printPlan(planParser = new SparkLogicalRelationParser(new DefaultExpressionParser()),
      builder = new PlanUmlDiagramBuilder(),
      entityName = s"_super_new_orders3",
      reportDescription = "",
      savePath = "examples",
      saver = new UmlPlanSaver())

  }

  def parse(plan: LogicalPlan): Array[(Origin, LogicalPlan)] = {
    val x: Array[(Origin, LogicalPlan)] = Array((plan.origin, plan))
    val y = plan.children.flatMap(parse).toArray
    x ++ y
  }

  "fdfd" should "fdfd" in {
    val users = Seq(("a", "b", "a", "a", "a", "a", "a", "a"))
      .toDF("user_id", "order_id", "a1", "a2", "a3", "a4", "a5", "a6")
      .as("USER")
      .cache()
      .filter(col("a2") === "a")
      .filter(col("a3") === "a")
      .cache()

    val resDF = users
      .filter(col("user_id") === "a")
      .union(users.filter(col("order_id") === "b"))
      .filter(col("a1") === "a")
    val logicNodes = parse(resDF.queryExecution.logical)
    val analyzedNodes = parse(resDF.queryExecution.analyzed)
    val cachedNodes = parse(resDF.queryExecution.withCachedData)
    val optimizedNodes = parse(resDF.queryExecution.optimizedPlan)

    println("fdfd")
  }

  "ffd" should "fdfd" in {
    val usersDF = Seq(("a", "b", "a", "a", "a", "a", "a", "a"))
      .toDF("user_id", "order_id", "name", "age", "job", "a4", "a5", "a6")

    val user1 = usersDF.select("user_id", "order_id")
    val user2 = usersDF.select("name", "age")

    val res = user1.join(user2, col("user_id") === col("name"))

    println("fdfd")


  }

//  "SparkUmlDiagram" should "build room example" in {
//    val userDF = Seq(("a", "b")).toDF("user_id", "user_name").as("USER_TABLE")
//    val phoneDF = Seq(("a", "b", "c")).toDF("phone_id", "user_id", "phone_number").as("PHONE_TABLE")
//    val roomDF = Seq(("a", "b", "c")).toDF("room_id", "phone_id", "room_number").as("ROOM_TABLE")
//
//    // find all rooms where 'alex' users live with phone_number starting with '+7952'
//    val alexUsers = userDF.filter(lower(col("user_name")) === "alex")
//    val filteredPhones = phoneDF.filter(col("phone_number").startsWith("+7952"))
//
//    alexUsers
//      .join(filteredPhones, Seq("user_id"), "inner")
//      .join(roomDF, Seq("phone_id"))
//      .select("room_id", "room_number")
////            .printPlan(parser = new DefaultExpressionParser,
////                       builder = new PlanUmlDiagramBuilder(),
////                       entityName = s"rooms",
////                       reportDescription = "find all rooms where 'alex' users live with phone_number starting with '+7952'",
////                       savePath = "examples",
////                       saver = new UmlPlanSaver())
////      .printPlan(parser = new DefaultExpressionParser,
////                 builder = new JsonDiagramBuilder(),
////                 entityName = s"rooms",
////                 reportDescription = "find all rooms where 'alex' users live with phone_number starting with '+7952'",
////                 savePath = "examples",
////                 saver = new LocalFilePlanSaver())
//      .printAsJson(
//        entityName = "rooms",
//        "find all rooms where 'alex' users live with phone_number starting with '+7952'",
//        savePath = "examples")
//  }

}

case class User(order_id: String,
                manager_user_id: String,
                user_id: String)
