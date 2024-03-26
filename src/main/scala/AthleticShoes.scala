import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoders, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Encoders

object AthleticShoes extends App with Context {
  override val appName: String = "3_2_Null_1"

  import spark.implicits._

  case class Shoes(
                    item_category: String,
                    item_name: String,
                    item_after_discount: String,
                    item_price: String,
                    percentage_solds: Int,
                    item_rating: Int,
                    item_shipping: String,
                    buyer_gender: String
                  )


  val shoesSchema = Encoders.product[Shoes].schema

  val shoesDF: DataFrame = spark.read.format("csv")
      .option("header", "true")
      .schema(shoesSchema)
      .load("src/main/resources/athletic_shoes.csv")



  def dropNulls(df: DataFrame) = {
    df.na.drop(Seq(
      "item_category",
      "item_name"))
  }

  def replaceNull(columnName: String, column: Column): Column =
    coalesce(col(columnName), column).as(columnName)

  def extractColumns(df: DataFrame): DataFrame = {
    df.select(
      col("item_category"),
      col("item_name"),
      replaceNull("item_after_discount", col("item_price")),
      replaceNull("item_rating", lit(0)),
      replaceNull("percentage_solds", lit(-1)),
      replaceNull("buyer_gender", lit("unknown")),
      replaceNull("item_price", lit("n/a")),
      replaceNull("item_shipping", lit("n/a"))
    )
  }

  val processedShoesDF = shoesDF
    .transform(dropNulls)
    .transform(extractColumns)

  val shoesDS = processedShoesDF.as[Shoes]

  shoesDS.show(10,false)


  spark.stop()

}
