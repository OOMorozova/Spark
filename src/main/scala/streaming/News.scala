package streaming
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// организовать потоковую обработку данных для получения количества новостей,
// каждый день размещаемых каждым сайтом в каждой категории

object News extends App with Context {
  override val appName: String = "2_3_2_News"

  val categoriesDf = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/news_category.csv")

  val bCategoriesDf = broadcast(categoriesDf)

  case class News(id: Integer,
                title: String,
                url: String,
                publisher: String,
                category: String,
                story: String,
                timestamp: BigInt
                )

  val newsSchema = Encoders.product[News].schema

  val streamDf = spark.readStream
    .schema(newsSchema)
    .option("header", "true")
    .csv("src/main/resources/news")

  def join2Df(df1: DataFrame, joinCols: Seq[String], joinType: String)(df2: DataFrame): DataFrame = {
    df2.as("df2")
      .join(df1.as("df1"), joinCols, joinType)
  }

  def calculateNewsCount(groupByCols: Seq[Column])(df: DataFrame) = {
    df
      .groupBy(
        groupByCols: _*)
      .agg(count("url").alias("news_count"))
  }

  val joinedDf = streamDf
    .transform(calculateNewsCount(Seq(
      date_trunc("day", (col("timestamp") / 1000).cast(TimestampType)).as("day"),
      col("publisher"),
      col("category"))
    ))
    .transform(join2Df(bCategoriesDf, Seq("category"), "left_outer"))
    .select("df2.day",
      "df2.publisher",
      "df1.category_name",
      "news_count")

 //так как количество новостей меняется в течении дня, то текущий день будет обновляться
  val writeQuery = joinedDf.writeStream
    .format("console")
    .option("truncate", "false")
    .outputMode(OutputMode.Update())
    .start()

  writeQuery.awaitTermination()


  spark.stop()
}