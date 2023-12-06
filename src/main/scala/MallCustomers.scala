import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SaveMode
// надеюсь, что верно поняла, что удалять Gender не нужно))
object MallCustomers extends App with Context {
  override val appName: String = "2_5_Practice_1"

  val Schema = StructType(Seq(
    StructField("CustomerID", IntegerType),
    StructField("Gender", StringType),
    StructField("Age", IntegerType),
    StructField("Annual Income (k$)", IntegerType),
    StructField("Spending Score (1-100)", IntegerType)
  ))


  val mallDF = spark.read.format("csv")
    .option("header", "true")
    .schema(Schema)
    .load("src/main/resources/mall_customers.csv")

  val isMale = when(lower(col("Gender")) === lit("male"), lit(1))
    .when(lower(col("Gender")) === lit("female"), lit(0))
    .otherwise(lit(-1))

  val incomeDF = (
    mallDF
      .withColumn("Age", col("Age") + lit(2))
      .filter((col("Age") >= 30) && (col("Age") <= 35))
      .groupBy(col("Gender"), col("Age"))
      .agg(round(avg(col("Annual Income (k$)")), 1).as("AVG Annual Income (k$)"))
      .sort(col("Gender"), col("Age"))
      .withColumn("gender_code", isMale)
    )

  incomeDF.show()

  incomeDF.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/customers")

  spark.stop()

}
