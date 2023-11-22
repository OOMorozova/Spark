import org.apache.spark.sql.types._
object Schema extends App with Context {
  override val appName: String = "2_1_Schema"

  val sc = spark.sparkContext

  val restaurantSchema = StructType(Seq(
    StructField("has_online_delivery", IntegerType),
    StructField("url", StringType),
    StructField("user_rating",
      StructType(Seq(
        StructField("rating_text", StringType),
        StructField("rating_color", StringType),
        StructField("votes", StringType),
        StructField("aggregate_rating", StringType)
      ))),
    StructField("name", StringType),
    StructField("cuisines", StringType),
    StructField("is_delivering_now", IntegerType),
    StructField("deeplink", StringType),
    StructField("menu_url", StringType),
    StructField("average_cost_for_two", LongType)
  ))
  val restaurantDF = spark.read
    .schema(restaurantSchema)
//    .option ("multiLine",true) # если бы данные в файле не были записаны в строчку, а были разбиты по строкам
    .json("src/main/resources/restaurant_ex.json")

  restaurantDF.show(false)
  if (restaurantDF.schema("is_delivering_now").dataType.typeName == "integer")
    println(" is_delivering_now is 'integer' column")
  if (restaurantDF.schema("has_online_delivery").dataType.typeName == "integer")
    println(" has_online_delivery is 'integer' column")

  restaurantDF
    .dtypes
    .foreach(x => println(s"Field \"${x._1}\" type ${x._2}"))

  spark.stop()

}
