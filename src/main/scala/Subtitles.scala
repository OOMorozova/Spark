import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Subtitles extends App with Context {
  override val appName: String = "2_5_Practice_2"

  object DfColumn extends DfColumn {
    implicit def columnToString(col: DfColumn.Value): String = col.toString
  }

  trait DfColumn extends Enumeration {
    val _c0, id,
    w_s1, w_s2,
    cnt_s1, cnt_s2 = Value
  }
  def read(file: String) = {

    spark.read
      .option("inferSchema", "true")
      .csv(file)

  }

  val s1Df = read("src/main/resources/subtitles_s1.json")
  val s2Df = read("src/main/resources/subtitles_s2.json")
  def withExplode(wordCol: String)(df: DataFrame): DataFrame = {
    val hasPattern = col("_c0").rlike("\\s*\"\\d+\"\\:\\s*\".*")
    df.filter(hasPattern)
      .withColumn(wordCol, explode(split(lower(col("_c0")), "\\W+")))
  }

  def extractWordGroups(wordCol: String,
                        cntCol: String)(df: DataFrame): DataFrame = {
    val hasWord = (coalesce(col(wordCol), lit("")) =!="") && col(wordCol).cast("int").isNull

    df
      .filter(hasWord)
      .groupBy(col(wordCol))
      .agg(count(col(wordCol)).as(cntCol))
      .orderBy(col(cntCol).desc)
      .limit(20)
  }

  def withId(df: DataFrame): DataFrame =
    df.withColumn(DfColumn.id, monotonically_increasing_id())

  def join2Df(df1: DataFrame)(df2:DataFrame): DataFrame = {
    val joinCondition = s1StatDf.col("id") === s2StatDf.col("id")

    (df2.as("s1")
      .join(df1.as("s2"), joinCondition, "inner")
      .select(col("s1.w_s1"),
        col("s1.cnt_s1"),
        col("s1.id"),
        col("s2.w_s2"),
        col("s2.cnt_s2"))
      )

  }

  val s1StatDf = s1Df
    .transform(withExplode(DfColumn.w_s1))
    .transform(extractWordGroups(DfColumn.w_s1, DfColumn.cnt_s1))
    .transform(withId)

  val s2StatDf = s2Df
    .transform(withExplode(DfColumn.w_s2))
    .transform(extractWordGroups(DfColumn.w_s2, DfColumn.cnt_s2))
    .transform(withId)


  val s3 = s1StatDf.transform(join2Df(s2StatDf))

  s3.show(25, false)

  s3.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save("resources/data/wordcount")


  spark.stop()

}
