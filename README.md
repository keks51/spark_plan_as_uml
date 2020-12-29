# visualizing spark plan as UML diagram

A library for drawing spark logic plan as UML.\
Using PlantUML.

## Requirements
spark 2.4.0 or higher\
for lower spark version some code changes should be applied\
Not tested with Datasets
## Examples

Room example
```scala
val userDF = Seq(("a", "b")).toDF("user_id", "user_name").as("USER_TABLE")
val phoneDF = Seq(("a", "b", "c")).toDF("phone_id", "user_id", "phone_number").as("PHONE_TABLE")
val roomDF = Seq(("a", "b", "c")).toDF("room_id", "phone_id", "room_number").as("ROOM_TABLE")

// find all rooms where 'alex' users live with phone_number starting with '+7952'
val alexUsers = userDF.filter(lower(col("user_name")) === "alex")
val filteredPhones = phoneDF.filter(col("phone_number").startsWith("+7952"))

alexUsers
  .join(filteredPhones, Seq("user_id"), "inner")
  .join(roomDF, Seq("phone_id"))
  .select("room_id", "room_number")
  .printPlanAsUml(parser = new DefaultExpressionParser,
                  entityName = s"rooms",
                  reportDescription = "",
                  savePath = "examples")
```
![Alt text](/examples/rooms.png)

In case of not implemented errors in DefaultExpressionParser or incorrect behavior you can extend, override and change 
logic in this class
