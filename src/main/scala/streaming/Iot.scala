package streaming

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._

//необходимо каждую минуту на протяжении двух минут отслеживать среднюю температуру (temp) окружающей среды
// (в градусах Цельсия), фиксируемую каждым устройством (device)

object Iot extends App with Context {
  override val appName: String = "2_3_1_Iot"

  case class Iot(ts: Double,
                 device: String,
                 co: Double,
                 humidity: Boolean,
                 light: Boolean,
                 lpg: Double,
                 motion: Boolean,
                 smoke: Double,
                 temp: Double,
                 id: Integer
                )

  val iotSchema = Encoders.product[Iot].schema

  val streamDf = spark.readStream
    .schema(iotSchema)
    .option("header", "true")
    .csv("src/main/resources/iot")

  def toCelsius(colName: String) = {
    (col(colName) - 32) * 5 / 9
  }
  def calculateAvgTemp(groupByCols: Seq[Column])(df: DataFrame) = {
    df
      .groupBy(
        groupByCols: _*)
      .agg(avg(toCelsius("temp")).alias("avg_temp"))
  }

  val slidingWindow = window(col("ts").cast(TimestampType), "2 minutes", "1 minutes")

  val transformedDf = streamDf
    .transform(calculateAvgTemp(Seq(col("device"),  slidingWindow)))


  val writeQuery = transformedDf.writeStream
    .format("console")
    .outputMode(OutputMode.Complete())
    .start()

  writeQuery.awaitTermination()

  spark.stop()
}