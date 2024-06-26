import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoder, Encoders}
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._

import scala.Int.MaxValue

object AiJobsIndustry extends App with Context {
  override val appName: String = "3_2_Transform_1"

  //part1 DF
  val aiJobsIndustryDF: DataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option ("multiLine", value = true)
    .csv("src/main/resources/AiJobsIndustry.csv")

  def dropNulls(df: DataFrame) = {
    df.na.drop()
  }

  def dropDupls(cols: Seq[String])(df: DataFrame) = {
    df.dropDuplicates(cols)
  }

  def extractNumReviews(df: DataFrame): DataFrame = {
    df.withColumn("NumReviews",
      regexp_replace(split(col("CompanyReviews"), "\\s")
        .getItem(0), "\\D","")
        .cast(IntegerType))
  }


  def extractLead(groupCol: String)(df: DataFrame): DataFrame = {
    df.groupBy(col(groupCol))
      .agg(sum("NumReviews").as("count_reviews"))
      .orderBy(desc("count_reviews"))
      .limit(1)
  }

  def join2Df(df1: DataFrame, joinCol: String)(df2: DataFrame): DataFrame = {
    val joinCondition = df1.col(joinCol)===df2.col(joinCol)

    df2.as("df2")
      .join(df1.as("df1"), joinCondition, "inner")
  }

  def extractReviewCount(groupCol1: String, groupCol2: String)(df: DataFrame): DataFrame = {
    df.groupBy(groupCol1, groupCol2)
      .agg(sum("NumReviews").as("count"))
      .na.fill(0)
  }

  def extractMinMax(groupCol: String, aggCol: String)(df: DataFrame): DataFrame = {
    df.groupBy(groupCol)
      .agg(
        min(aggCol).as("count_min"),
        max(aggCol).as("count_max")
      )
  }

  def joinMinMax(df1: DataFrame, joinCol: String)(df2: DataFrame): DataFrame = {
    val joinCondition = (df1.col(joinCol) === df2.col(joinCol)) &&
      (df1.col("count") === df2.col("count_min") ||
        df1.col("count") === df2.col("count_max"))

    df2.as("df2")
      .join(df1.as("df1"), joinCondition, "inner")
      .select("df1.*",
        "count_min",
        "count_max")
  }

  def withCountType(df: DataFrame): DataFrame = {
    val isMax = col("count") === col("count_max")
    df.withColumn("count_type", when(isMax, lit("max")).otherwise(lit("min")))
  }

  def extractColumns(statsType: String)(df: DataFrame): DataFrame = {
    df.select(col(statsType).as("name"),
              lower(regexp_extract(lit(statsType), "(^.[^A-Z]*)", 1)).as("stats_type"),
              col("Location").as("location"),
              col("count"),
              col("count_type")
    )
  }

  def merge2Df(df1: DataFrame)(df2: DataFrame): DataFrame = {
    df1.unionByName(df2)
  }

  def collectCol(seqCol: Seq[Column], collectCol: String)(df: DataFrame): DataFrame = {
    df.groupBy(seqCol:_*)
      .agg(collect_set(collectCol).as(collectCol))
      .select("name",
        "stats_type",
        "location",
        "count",
        "count_type")
  }


  val df1 = aiJobsIndustryDF
    .transform(dropNulls)
    .transform(dropDupls(Seq(
      "Company",
      "JobTitle",
      "Link")))
    .transform(extractNumReviews)

  val companyDF = df1
    .transform(extractLead("Company"))


  val companyLocDF = df1
    .transform(join2Df(companyDF, "Company"))
    .transform(extractReviewCount("df1.Company", "Location"))


  val statsCompanyDF = companyLocDF
    .transform(extractMinMax("df1.Company", "count"))
    .transform(joinMinMax(companyLocDF, "Company"))
    .transform(withCountType)
    .transform(extractColumns("Company"))


  val jobDF = df1
    .transform(extractLead("JobTitle"))


  val jobLocDF = df1
    .transform(join2Df(jobDF, "JobTitle"))
    .transform(extractReviewCount("df1.JobTitle", "Location"))

  val statsJobDF = jobLocDF
    .transform(extractMinMax("df1.JobTitle", "count"))
    .transform(joinMinMax(jobLocDF, "JobTitle"))
    .transform(withCountType)
    .transform(extractColumns("JobTitle"))

  val statsDF = statsJobDF
    .transform(merge2Df(statsCompanyDF))
    .transform(collectCol(
      Seq(col("name"), col("stats_type"), col("count"),
        col("count_type")), "location"))

  //part2 DS
  case class JobsIndustry(
                           JobTitle: String,
                           Company: String,
                           Location: String,
                           CompanyReviews: String,
                           Link: String
                    )

  import spark.implicits._
  val aiJobsIndustryDS = aiJobsIndustryDF.as[JobsIndustry]
  
  case class JobIndustryNum(
                             JobTitle: String,
                             Company: String,
                             Location: String,
                             NumReviews: Int
                           )
  
  case class GroupInfo(
                        name: String,
                        countReviews: Int
                      )

  case class NameLocInfo(
                          name: String,
                          location: String,
                          count: Int
                        )

  case class AggInfo(name: String,
                     count_max: Int,
                     count_min: Int
                    )

  case class StatsInfo(name: String,
                       stats_type: String,
                       location: Array[String],
                       count: Int,
                       count_type: String
                      )

  def extractIntRewiew(CompanyReviews: String): Integer = {
    val strReviews = Option(CompanyReviews.split("\\s").head).getOrElse(0)
    strReviews.toString.replaceAll("\\D", "").toInt
  }
  def extractReviewNum(ds: Dataset[JobsIndustry]):  Dataset[JobIndustryNum] = {
    ds.map(line => {
      JobIndustryNum(line.JobTitle,
        line.Company,
        line.Location,
        extractIntRewiew(line.CompanyReviews))
    }
    )
  }

  def filterNull(ds: Dataset[JobsIndustry]): Dataset[JobsIndustry] = {
    ds.filter(s =>
      s.Company != null &&
        s.JobTitle != null &&
        s.Location != null &&
        s.CompanyReviews != null &&
        s.Link != null)
  }

  def filterDuplicates(ds: Dataset[JobsIndustry]): Dataset[JobsIndustry] = {
    ds.dropDuplicates(Seq("Company", "JobTitle", "Link"))
  }

  def findLeader(key: JobIndustryNum => String)(ds: Dataset[JobIndustryNum]): Dataset[GroupInfo]  = {

     val reviewCounter = new Aggregator[JobIndustryNum, Int, Int] {

       override def zero: Int = 0

       override def reduce(b: Int, a: JobIndustryNum): Int = b + a.NumReviews

       override def merge(b1: Int, b2: Int): Int = b1 + b2

       override def finish(reduction: Int): Int = reduction

       override def bufferEncoder: Encoder[Int] = Encoders.scalaInt

       override def outputEncoder: Encoder[Int] = Encoders.scalaInt
     }
       .toColumn.name("countReviews")

      ds.groupByKey(key)
        .agg(reviewCounter)
        .map(r => GroupInfo(r._1, r._2))
        .orderBy(desc("countReviews"))
        .limit(1)
  }

  def join2Ds(ds2: Dataset[GroupInfo], joinCol: String)(ds1: Dataset[JobIndustryNum]): Dataset[JobIndustryNum] = {
    ds1.joinWith(
        ds2,
        ds1.col(joinCol) === ds2.col("name"),
        "inner"
      )
      .map(record =>
        JobIndustryNum(
          record._1.JobTitle,
          record._1.Company,
          record._1.Location,
          record._1.NumReviews
        )
      )

  }

  def findNumTotal(key: JobIndustryNum => (String, String))(ds: Dataset[JobIndustryNum]): Dataset[NameLocInfo] = {
    val numTotal = new Aggregator[JobIndustryNum, Int, Int] {

      override def zero: Int = 0

      override def reduce(b: Int, a: JobIndustryNum): Int = b + a.NumReviews

      override def merge(b1: Int, b2: Int): Int = b1 + b2

      override def finish(reduction: Int): Int = reduction

      override def bufferEncoder: Encoder[Int] = Encoders.scalaInt

      override def outputEncoder: Encoder[Int] = Encoders.scalaInt
    }
      .toColumn.name("numTotal")

    ds.groupByKey(key)
      .agg(numTotal)
      .map(r => NameLocInfo(r._1._1, r._1._2, r._2))
  }

  def findMinMaxCount(key: NameLocInfo => String)(ds: Dataset[NameLocInfo]): Dataset[AggInfo] = {
    val count_max =
      new Aggregator[NameLocInfo, Int, Int] {
        override def zero: Int = 0

        override def reduce(b: Int, a: NameLocInfo): Int = if (a.count > b) a.count else b

        override def merge(b1: Int, b2: Int): Int = if (b1 > b2) b1 else b2

        override def finish(reduction: Int): Int = reduction

        override def bufferEncoder: Encoder[Int] = Encoders.scalaInt

        override def outputEncoder: Encoder[Int] = Encoders.scalaInt
      }
        .toColumn.name("count_max")

    val count_min = new Aggregator[NameLocInfo, Int, Int] {
      override def zero: Int = MaxValue

      override def reduce(b: Int, a: NameLocInfo): Int = if (b < a.count) b else a.count

      override def merge(b1: Int, b2: Int): Int = if (b1 < b2) b1 else b2

      override def finish(reduction: Int): Int = reduction

      override def bufferEncoder: Encoder[Int] = Encoders.scalaInt

      override def outputEncoder: Encoder[Int] = Encoders.scalaInt
    }
      .toColumn.name("count_min")

    ds.groupByKey(key)
      .agg(count_max,count_min)
      .map(r => AggInfo(r._1, r._2, r._3))
  }

  def joinWithAgg(ds2: Dataset[NameLocInfo])(ds1: Dataset[AggInfo]): Dataset[(AggInfo, NameLocInfo)] = {
    ds1.as("ds1").joinWith(ds2.as("ds2"),
      col("ds2.name") === col("ds1.name") &&
        (col("ds2.count") === col("ds1.count_min") ||
          col("ds2.count") === col("ds1.count_max")),
      "inner")
  }

  def countType(count_max: Int, count: Int): String = {
    if (count_max == count) "max"
    else "min"
  }

  def selectStatInfo(stat_type: String)(ds: Dataset[(AggInfo, NameLocInfo)]): Dataset[StatsInfo] = {
    ds.map(record =>
      StatsInfo(
        record._2.name,
        stat_type,
        Array(record._2.location),
        record._2.count,
        countType(record._1.count_max, record._2.count)
      )
    )
  }

  def groupWithCollect(ds: Dataset[StatsInfo]): Dataset[StatsInfo] = {
    ds.groupByKey(rec => (rec.name, rec.stats_type, rec.count, rec.count_type))
      .mapGroups { (seq, iter) =>
        val collectCol = iter.flatMap(_.location).toArray
        StatsInfo(
          seq._1,
          seq._2,
          collectCol,
          seq._3,
          seq._4
        )
      }
  }

  val  cleanedJobsIndustryDS = aiJobsIndustryDS
    .transform(filterNull)
    .transform(filterDuplicates)
    .transform(extractReviewNum)
    .as("ds1")

  val companyDS =  cleanedJobsIndustryDS
    .transform(findLeader(_.Company))

  val companyLocDS =  cleanedJobsIndustryDS
    .transform(join2Ds(companyDS, "Company"))
    .transform(findNumTotal(gr => (gr.Company, gr.Location)))

  val aggCompanyDS = companyLocDS
    .transform(findMinMaxCount(_.name))

  val statsCompanyDS = aggCompanyDS
    .transform(joinWithAgg(companyLocDS))
    .transform(selectStatInfo("company"))

  val jobDS =  cleanedJobsIndustryDS
    .transform(findLeader(_.JobTitle))

  val jobLocDS =  cleanedJobsIndustryDS
    .transform(join2Ds(jobDS, "JobTitle"))
    .transform(findNumTotal(gr => (gr.JobTitle, gr.Location)))

  val aggJobDS = jobLocDS
    .transform(findMinMaxCount(_.name))

  val statsJobDS = aggJobDS
    .transform(joinWithAgg(jobLocDS))
    .transform(selectStatInfo("job"))
  
  val statsDS = statsJobDS
    .unionByName(statsCompanyDS)
    .transform(groupWithCollect)

  //  statsDF.show(20, false)
  statsDS.show(20, truncate = false)

  spark.stop()

}
