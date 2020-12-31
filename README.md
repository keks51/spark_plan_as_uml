# visualizing spark plan as UML diagram

A library for drawing spark logic plan as UML.\
Using PlantUML.

## Requirements

spark 2.4.0 or higher\
for lower spark version some code changes should be applied\
Not tested with Datasets

## MAVEN
```xml
<dependency>
    <groupId>io.github.keks51</groupId>
    <artifactId>spark-plan-as-uml</artifactId>
    <version>1.0.6</version>
</dependency>
```
## Examples

Room code example

```scala
import com.keks.plan.builder.{JsonDiagramBuilder, PlanUmlDiagramBuilder}
import com.keks.plan.parser.DefaultExpressionParser
import com.keks.plan.write.{LocalFilePlanSaver, UmlPlanSaver}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType


val spark: SparkSession = _

import spark.implicits._


val userDF = Seq(("a", "b")).toDF("user_id", "user_name").as("USER_TABLE")
val phoneDF = Seq(("a", "b", "c")).toDF("phone_id", "user_id", "phone_number").as("PHONE_TABLE")
val roomDF = Seq(("a", "b", "c")).toDF("room_id", "phone_id", "room_number").as("ROOM_TABLE")

// find all rooms where 'alex' users live with phone_number starting with '+7952'
val alexUsers = userDF.filter(lower(col("user_name")) === "alex")
val filteredPhones = phoneDF.filter(col("phone_number").startsWith("+7952"))
val result = alexUsers
  .join(filteredPhones, Seq("user_id"), "inner")
  .join(roomDF, Seq("phone_id"))
  .select("room_id", "room_number")
```

PlantUml

```scala

result.printPlan(parser = new DefaultExpressionParser,
                 builder = new PlanUmlDiagramBuilder(),
                 entityName = s"rooms",
                 reportDescription = "find all rooms where 'alex' users live with phone_number starting with '+7952'",
                 savePath = "examples",
                 saver = new UmlPlanSaver())
```

![Alt text](/examples/rooms.png)

JSON

```scala
result.printPlan(parser = new DefaultExpressionParser,
                 builder = new JsonDiagramBuilder(),
                 entityName = s"rooms",
                 reportDescription = "find all rooms where 'alex' users live with phone_number starting with '+7952'",
                 savePath = "examples",
                 saver = new LocalFilePlanSaver())
```

```json
{
  "entityName": "rooms",
  "reportDescription": "find all rooms where 'alex' users live with phone_number starting with '+7952'",
  "edges": [
    {"from": 1, "to": 3}, {"from": 3, "to": 5}, {"from": 3, "to": 14}, {"from": 5, "to": 6},
    {"from": 5, "to": 10}, {"from": 6, "to": 7}, {"from": 10, "to": 11}
  ],
  "nodes": [
    {
      "id": 1,
      "name": "SELECT",
      "desc": "room_id\nroom_number"
    },
    {
      "id": 3,
      "name": "JOIN",
      "desc": "INNER\nPHONE_TABLE.phone_id == ROOM_TABLE.phone_id"
    },
    {
      "id": 5,
      "name": "JOIN",
      "desc": "INNER\nUSER_TABLE.user_id == PHONE_TABLE.user_id"
    },
    {
      "id": 6,
      "name": "FILTER",
      "desc": "Lower[user_name] == alex"
    },
    {
      "id": 7,
      "name": "NAMED_SOURCE_TABLE",
      "desc": "SourceType: GENERATED TABLE\nTableName: USER_TABLE\nuser_id: StringType\nuser_name: StringType"
    },
    {
      "id": 10,
      "name": "FILTER",
      "desc": "phone_number StartsWith[+7952]"
    },
    {
      "id": 11,
      "name": "NAMED_SOURCE_TABLE",
      "desc": "SourceType: GENERATED TABLE\nTableName: PHONE_TABLE\nphone_id: StringType\nuser_id: StringType\nphone_number: StringType"
    },
    {
      "id": 14,
      "name": "NAMED_SOURCE_TABLE",
      "desc": "SourceType: GENERATED TABLE\nTableName: ROOM_TABLE\nroom_id: StringType\nphone_id: StringType\nroom_number: StringType"
    }
  ]
}
```
Use pretty prints
```scala
.printAsUml(
  entityName = "rooms",
  "find all rooms where 'alex' users live with phone_number starting with '+7952'",
  savePath = "examples")
  
.printAsJson(
  entityName = "rooms",
  "find all rooms where 'alex' users live with phone_number starting with '+7952'",
  savePath = "examples")
```
## NOTICE

In case of not implemented errors in DefaultExpressionParser or incorrect behavior you can extend, override and change
logic in this class
