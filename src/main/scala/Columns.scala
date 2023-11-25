import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
//из загруженного дата-фрейма должны быть выбраны следующие колонки: Hour, TEMPERATURE, HUMIDITY, WIND_SPEED
//на экран должны быть выведены первые три строчки данных из выбранных колонок
object Columns extends App with Context {
  override val appName: String = "2_3_Columns"

  val bikeSharingDF = spark.read.format("csv")
    .option("header", "true")
    .load("src/main/resources/bike_sharing.csv")

  val newDF = bikeSharingDF.select(
    col("Hour"),
    col("TEMPERATURE"),
    col("HUMIDITY"),
    col("WIND_SPEED")
  )

  newDF.show(3)

  spark.stop()

}
