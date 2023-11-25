import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
object playground extends App {
   val spark=SparkSession.builder()
        .appName("Playground")
        .master("local")
        .getOrCreate()
   val sc = spark.sparkContext
//functions
  val bikeSharingDF = spark.read.format("csv").option("header", "true").load("src/main/resources/bike_sharing.csv")

  import spark.implicits._
  val newDF = bikeSharingDF.select(
    bikeSharingDF.col("Date"),
    col("Date"),
    column("Date"),
    Symbol("Date"),
    $"Date",
    expr("Date")
  )

  newDF.show(2)

  val bikes = bikeSharingDF.select("*").count()
  println(s"number of records $bikes")
  val distinctEntries = bikeSharingDF.select("HOLIDAY").distinct().count()
  println(s"distinct values in column HOLIDAYS  $distinctEntries") // distinct values in column HOLIDAYS  2
  val bikesWithColumnRenamed = bikeSharingDF.withColumnRenamed("TEMPERATURE", "Temp")
  bikesWithColumnRenamed.show(3)
  bikeSharingDF.drop("Date", "Hour").show(3)
  bikeSharingDF
    .withColumn(
      "is_holiday",
      when(col("HOLIDAY") === "No Holiday", false)
        .when(col("HOLIDAY") === "Holiday", true)
        .otherwise(null)
    ).show(3)

  bikeSharingDF
    .groupBy("Date")
    .agg(
      sum("RENTED_BIKE_COUNT").as("bikes_total"))
    .orderBy("Date")
    .show(3)

  val res = bikeSharingDF
    .filter(col("RENTED_BIKE_COUNT")===254)
    .filter(col("TEMPERATURE")>0).count()

  println("Answer", res)


  //iris
  val iris = spark.read
    .format("json")
    .option("inferSchema", "true")
//    .option("multiline", "true")
    .load("src/main/resources/iris.json")
  iris.show(2)
  iris.printSchema()

  val irisArray: Array[Row] = iris.take(1)
  irisArray.foreach(println)

//test
  import spark.implicits._
   val courses=Seq(
        ("Scala",22),
        ("Spark",30)
        )

  val coursesDF=courses.toDF("title","duration (h)")
  coursesDF.show()
  spark.stop()

}
