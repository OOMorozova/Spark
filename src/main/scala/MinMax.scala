import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// Рассчитывается минимальное и максимальное значение температуры TEMPERATURE для каждой даты Date.
// Результат расчетов записывается в колонки min_temp, max_temp и выводится на экран.
// https://stepik.org/lesson/692632/step/7?unit=692222

object MinMax extends App with Context {
  override val appName: String = "2_3_MinMax"

  val bikeSchema = StructType(Seq(
    StructField("Date", DateType),
    StructField("RENTED_BIKE_COUNT", StringType),
    StructField("Hour", IntegerType),
    StructField("TEMPERATURE", StringType),
    StructField("HUMIDITY", IntegerType),
    StructField("WIND_SPEED", DoubleType),
    StructField("Visibility", IntegerType),
    StructField("DEW_POINT_TEMPERATURE", DoubleType),
    StructField("SOLAR_RADIATION", DoubleType),
    StructField("RAINFALL", DoubleType),
    StructField("Snowfall", DoubleType),
    StructField("SEASONS", StringType),
    StructField("HOLIDAY", StringType),
    StructField("FUNCTIONING_DAY", StringType)
  ))

  val bikeSharingDF = spark.read.format("csv")
    .option("header", "true")
    .schema(bikeSchema)
    .option("dateFormat", "dd/MM/yyyy")
    .load("src/main/resources/bike_sharing.csv")


  val newDF = bikeSharingDF
    .withColumn(
      "TEMPERATURE",
      col("TEMPERATURE").cast(DoubleType)
    )
    .groupBy(col("Date"))
    .agg(min(col("TEMPERATURE")).as("min_temp"),
      max(col("TEMPERATURE")).as("max_temp")
    )
    .orderBy(col("Date"))

  newDF.show()

  spark.stop()

}
