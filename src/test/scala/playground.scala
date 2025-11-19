//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoder, Encoders}
object playground extends App {
  /*
  id,name,gender,birthday,country_code,age,dep_code,avgReward,salary,department
  1,Alice,1,2000-01-02,234,30,1,3456,10000,Finance
  2,Sam,0,2000-01-02,345,23,1,3456,10000,Finance
  3,Tom,0,2000-01-02,67,40,2,345,500,HR
  4,Niki,1,2000-01-02,345,19,2,3456,500,HR
  */

  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types._
  import org.apache.spark.sql.Encoders
  import org.apache.spark.sql.streaming.OutputMode
  val spark = SparkSession.builder().appName("2_3_1").master("local").getOrCreate()

  case class  Iot(ts: Double,
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
  //
//  case class Employee(id: Integer,
//                      name: String,
//                      gender: Boolean,
//                      birthday: String,
//                      country_code: Integer,
//                      age: Integer,
//                      dep_code: Integer,
//                      avgReward: Double,
//                      salary: Integer,
//                      department: String
//                     )
//
//
//  val employeeSchema = Encoders.product[Employee].schema
//
//  val DF = (spark.readStream
////      .format("csv")
//      .schema(employeeSchema)
//      .option("header", "true")
//      .csv("src/main/resources/dir")
//      )

//  DF.show()
//  val res = DF
//    .groupBy("department")
//    .agg(avg("salary"))
//
//
//  res.show(10,false)
//  res.explain()


//  val spark = SparkSession.builder().appName("5_3_2_Shell").master("local").getOrCreate()
//  val employeesDF = (spark.read
//    .format("csv")
//    .option("inferSchema", "true")
//    .option("header", "true")
//    .load("src/main/resources/employee.csv")
//    )
//
//  def withNameLength(df: DataFrame): DataFrame = {
//    df.withColumn("length", length(col("name")))
//  }
//
//  def extractColumns(df: DataFrame): DataFrame = {
//    df.select("name", "length")
//  }
//
//  val resDF =
//    (employeesDF
//      .transform(withNameLength)
//      .transform(extractColumns)
//      )
//
//  resDF.show(50, false)
//  val spark = SparkSession.builder().appName("5_3_1_Shell").master("local").getOrCreate()
//  val sc = spark.sparkContext
//  val shellRdd = sc.range(0L, 100000L)
//  val numPartitions = shellRdd.getNumPartitions
//  println(s"partitions = ${numPartitions}")
//  val sumElem = shellRdd.sum()
//  val fmt = new java.text.DecimalFormat("#,##0.##############")
//  println(s"sum of elements = ${fmt.format(sumElem)}")
//  shellRdd.take(5)
//
//  val joinedStreamDf = (employeesDF
//    .join(
//      DF, // объединяем с обычным датафреймом
//      DF.col("name") === employeesDF.col("name"),
//      "right")
////    .select("Title", "Department", "dept_name")
//    )


//  val joinQuery = joinedStreamDf
//    .writeStream
//    .format("console")
//    .outputMode("append")
//    .start()
//
//  joinQuery.awaitTermination()
//  spark.stop()
}
//import org.apache.spark.sql.{DataFrame, Row, SparkSession}
//import org.apache.spark.sql.types._
//import org.apache.spark.sql.functions._
//
//
//object playground extends App {
//  val spark=SparkSession.builder()
//        .appName("Playground")
//        .master("local")
//        .getOrCreate()
//  val sc = spark.sparkContext
//
//  val testDF: DataFrame = spark.read
//    .option("header", "true")
//    .option("inferSchema", "true")
//    .option("mergeSchema", "true")
//    .csv("src/main/resources/test.csv")
//
//  val cols = testDF.columns.toList.tail
//  print(cols)
//
//  val resDF = testDF
//    .na.fill(0)
//    .withColumn("map", map_concat(cols))
//
//  testDF.show(10,false)
//  resDF.show(10,false)
//
////  val channelsDF: DataFrame = spark.read
////    .option("header", "true")
////    .option("inferSchema", "true")
////    .csv("src/main/resources/channel.csv")
////
////  case class Channel(
////                      channel_name: String,
////                      city: String,
////                      country: String,
////                      created: String,
////                    )
////
////  import spark.implicits._
////
////
////  channelsDS.toDF().show(10,false)
////
////  //join
////  val valuesDF = spark.read
////    .option("inferSchema", "true")
////    .option("header", "true")
////    .csv("src/main/resources/stack_link_value.csv")
////  val tagsDF = spark.read
////    .option("inferSchema", "true")
////    .option("header", "true")
////    .csv("src/main/resources/stack_links.csv")
////
//////functions
////  val bikeSharingDF = spark.read.format("csv").option("header", "true").load("src/main/resources/bike_sharing.csv")
////
////  import spark.implicits._
////  val newDF = bikeSharingDF.select(
////    bikeSharingDF.col("Date"),
////    col("Date"),
////    column("Date"),
////    Symbol("Date"),
////    $"Date",
////    expr("Date")
////  )
////
////  newDF.show(2)
////
////  val bikes = bikeSharingDF.select("*").count()
////  println(s"number of records $bikes")
////  val distinctEntries = bikeSharingDF.select("HOLIDAY").distinct().count()
////  println(s"distinct values in column HOLIDAYS  $distinctEntries") // distinct values in column HOLIDAYS  2
////  val bikesWithColumnRenamed = bikeSharingDF.withColumnRenamed("TEMPERATURE", "Temp")
////  bikesWithColumnRenamed.show(3)
////  bikeSharingDF.drop("Date", "Hour").show(3)
////  bikeSharingDF
////    .withColumn(
////      "is_holiday",
////      when(col("HOLIDAY") === "No Holiday", false)
////        .when(col("HOLIDAY") === "Holiday", true)
////        .otherwise(null)
////    ).show(3)
////
////  bikeSharingDF
////    .groupBy("Date")
////    .agg(
////      sum("RENTED_BIKE_COUNT").as("bikes_total"))
////    .orderBy("Date")
////    .show(3)
////
////  val res = bikeSharingDF
////    .filter(col("RENTED_BIKE_COUNT")===254)
////    .filter(col("TEMPERATURE")>0).count()
////
////  println("Answer", res)
////
////
////  //iris
////  val iris = spark.read
////    .format("json")
////    .option("inferSchema", "true")
//////    .option("multiline", "true")
////    .load("src/main/resources/iris.json")
////  iris.show(2)
////  iris.printSchema()
////
////  val irisArray: Array[Row] = iris.take(1)
////  irisArray.foreach(println)
////
//////test
////  import spark.implicits._
////   val courses=Seq(
////        ("Scala",22),
////        ("Spark",30)
////        )
////
////  val coursesDF=courses.toDF("title","duration (h)")
////  coursesDF.show()
//  spark.stop()
//
//}
//
