import org.apache.spark.sql.functions._
//создать колонку is_workday, состоящую из значений 0 и 1
// [значение 0 - если значением колонки HOLIDAY является Holiday и значением колонки FUNCTIONING_DAY является No ].
//
//Выведите на экран строчки, которые включают в себя только уникальные значения из колонок  "HOLIDAY", "FUNCTIONING_DAY", "is_workday"
object Uniq extends App with Context {
  override val appName: String = "2_3_Uniq"

  val bikeSharingDF = spark.read.format("csv")
    .option("header", "true")
    .load("src/main/resources/bike_sharing.csv")

  val newDF = bikeSharingDF
    .withColumn("is_workday", when((col("HOLIDAY") === "Holiday") &&
      (col("FUNCTIONING_DAY") === "No"), 0)
      .otherwise(1))
    .select(col("HOLIDAY"),
      col("FUNCTIONING_DAY"),
      col("is_workday")).distinct()

  newDF.show()

  spark.stop()

}
