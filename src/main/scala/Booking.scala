
import org.apache.spark.sql.functions._
import org.apache.spark.sql._


object Booking extends App with Context {
  override val appName: String = "6_5_Optimize_1"

  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

  def readCSV(path: String) = {
    spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(path)

  }


  val codesDF = readCSV("src/main/resources/cancel_codes.csv")


  val hotelDF = readCSV("src/main/resources/hotel_bookings.csv")

  def join2Df(df1: DataFrame, joinCol1: String, joinCol2: String)(df2: DataFrame): DataFrame = {
    val joinCondition = col(joinCol1) === col(joinCol2)
    df2.as("df2")
      .join(df1.as("df1"), joinCondition, "left")

  }

  def filterWrong(df: DataFrame): DataFrame = {
    val condition = ((col("reservation_status") === lit("Canceled")) &&
      (col("is_cancelled") === lit("no"))) ||
      ((col("reservation_status")  =!= "Canceled") &&
        col("is_cancelled") === lit("yes"))

    df.filter(condition)

  }

  val badDF =
    hotelDF
      .transform(join2Df(codesDF, "df1.id","df2.is_canceled"))
      .transform(filterWrong)
 val resBad = badDF.count()

  println(resBad)


 def selectCols(df: DataFrame): DataFrame = {
     df.select("is_canceled", "reservation_status")
  }

  val goodDF =
    hotelDF
      .transform(selectCols) //сокращаем количество колонок
      .transform(join2Df(broadcast(codesDF), "df1.id", "df2.is_canceled")) //broadcastJoin
      .transform(filterWrong)
  val resGood = goodDF.count()

  println(resGood)

  System.in.read()

  spark.stop()

}
