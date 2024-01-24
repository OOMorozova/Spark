import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Subtitles extends App with Context {
  override val appName: String = "2_5_Practice_2"
  val s1 = spark.read
    .option("inferSchema", "true")
    .csv("src/main/resources/subtitles_s1.json")

  val s2 = spark.read
    .option("inferSchema", "true")
    .csv("src/main/resources/subtitles_s2.json")

  def withExplode(df: DataFrame): DataFrame = {
    val hasPattern = col("_c0").rlike("\\s*\"\\d+\"\\:\\s*\".*")
    df.filter(hasPattern)
      .withColumn("w", explode(split(lower(col("_c0")), "\\W+")))
  }

  def extractWordGroups(df: DataFrame): DataFrame = {
    val hasWord = (coalesce(col("w"), lit("")) =!="") && col("w").cast("int").isNull

    df
      .filter(hasWord)
      .groupBy(col("w"))
      .agg(count(col("w")).as("cnt"))
      .orderBy(col("cnt").desc)
      .limit(20)
  }

  def withId(df: DataFrame): DataFrame =
    df.withColumn("id", monotonically_increasing_id())

  val s1_stat = (s1
    .transform(withExplode)
    .transform(extractWordGroups)
    .transform(withId)
    )

  val s2_stat = (s2
    .transform(withExplode)
    .transform(extractWordGroups)
    .transform(withId)
    )



  val joinCondition = s1_stat.col("id") === s2_stat.col("id")

  val s3 = (s1_stat.as("s1")
    .join(s2_stat.as("s2"), joinCondition, "inner")
    .select(col("s1.w").as("w_s1"), col("s1.cnt").as("cnt_s1"), col("s1.id"), col("s2.w").as("w_s2"), col("s2.cnt").as("cnt_s2"))
    )

  s3.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save("resources/data/wordcount")


  spark.stop()

}
