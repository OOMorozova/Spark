import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._

object struct_2_3_2 extends App {

  val spark = SparkSession.builder().appName("2_3_1").master("local").getOrCreate()

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

  val Df = spark.readStream
    .schema(iotSchema)
    .option("header", "true")
    .csv("src/main/resources/iot")
  val winDf = Df
    .withColumn("timestamp", col("ts").cast(TimestampType))
    .groupBy(col("device"),
      window(
        col("timestamp"),
        "2 minutes",
        "1 minutes")
    )
    .agg(
      avg("temp").alias("avg_temp"))
  val writeQuery = winDf.writeStream
    .format("console")
    .outputMode(OutputMode.Complete())
    .start()

  writeQuery.awaitTermination()

  spark.stop()
}