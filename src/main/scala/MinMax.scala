import org.apache.spark.sql.functions._

// Рассчитывается минимальное и максимальное значение температуры TEMPERATURE для каждой даты Date.
// Результат расчетов записывается в колонки min_temp, max_temp и выводится на экран.


object MinMax extends App with Context {
  override val appName: String = "2_3_MinMax"

  val bikeSharingDF = spark.read.format("csv")
    .option("header", "true")
    .load("src/main/resources/bike_sharing.csv")

  val newDF = bikeSharingDF
    .groupBy(col("Date"))
    .agg(min(col("TEMPERATURE")).as("min_temp"),
      max(col("TEMPERATURE")).as("max_temp")
    )

  newDF.show()

  spark.stop()

}
