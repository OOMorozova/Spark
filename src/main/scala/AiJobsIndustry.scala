import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset, DatasetHolder, Encoder, Encoders}
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._


object AiJobsIndustry extends App with Context {
  override val appName: String = "3_2_Transform_1"

  //part1 DS
  val aiJobsIndustryDF: DataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option ("multiLine", true)
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

    (df2.as("df2")
      .join(df1.as("df1"), joinCondition, "inner")
      )
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

    (df2.as("df2")
      .join(df1.as("df1"), joinCondition, "inner")
      .select("df1.*",
        "count_min",
        "count_max")
      )
  }

  def withCountType(df: DataFrame): DataFrame = {
    val isMax =(col("count") === col("count_max"))
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
    //избыточная трансформация?
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
    .transform(extractLead(("Company")))


  val companyLocDF = df1
    .transform(join2Df(companyDF, "Company"))
    .transform(extractReviewCount("df1.Company", "Location"))


  val statsCompanyDF = (companyLocDF
    .transform(extractMinMax("df1.Company", "count"))
    .transform(joinMinMax(companyLocDF, "Company"))
    .transform(withCountType)
    .transform(extractColumns("Company"))
  )


  val jobDF = (df1
    .transform(extractLead("JobTitle"))
    )


  val jobLocDF = (df1
    .transform(join2Df(jobDF, "JobTitle"))
    .transform(extractReviewCount("df1.JobTitle", "Location"))
    )

  val statsJobDF = (jobLocDF
    .transform(extractMinMax("df1.JobTitle", "count"))
    .transform(joinMinMax(jobLocDF, "JobTitle"))
    .transform(withCountType)
    .transform(extractColumns("JobTitle"))
    )

  val statsDF = statsJobDF
    .transform(merge2Df(statsCompanyDF))
    .transform(collectCol(
      Seq(col("name"), col("stats_type"), col("count"),
        col("count_type")), "location"))


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

  def extractNumDS(line: JobsIndustry): JobIndustryNum = {
    val strReviews = Option(line.CompanyReviews.split("\\s").head).getOrElse(0)
    val numReviews = strReviews.toString.replaceAll("\\D","").toInt
    JobIndustryNum(line.JobTitle, line.Company, line.Location, numReviews)
  }


 val ds1 = (aiJobsIndustryDS
   .filter(s =>
       s.Company != null &&
       s.JobTitle != null &&
       s.Location != null &&
       s.CompanyReviews != null &&
       s.Link != null)
   .dropDuplicates(Seq("Company", "JobTitle", "Link"))
   .map(line => extractNumDS(line))
   .as("ds1")
   )

  case class GroupInfo(
                        name: String,
                        countReviews: Int
                      )

  def findLeader(key: JobIndustryNum => String)(ds: Dataset[JobIndustryNum]) = {
      ds.groupByKey(key)
      .agg(new Aggregator[JobIndustryNum, Int, Int] {
        // с чего начинаем вычисления
        override def zero: Int = 0

        //вычисление промежуточных результатов
        override def reduce(b: Int, a: JobIndustryNum): Int = b + a.NumReviews

        // объединяем промежуточные результаты, полученные в разных партициях
        override def merge(b1: Int, b2: Int): Int = b1 + b2

        // финальный результат
        override def finish(reduction: Int): Int = reduction

        override def bufferEncoder: Encoder[Int] = Encoders.scalaInt

        //если, например, в качестве выходного типа используется case class IndustryLeader,
        //то код будет следующий
        // override def outputEncoder: Encoder[IndustryLeader] = Encoders.product[IndustryLeader]
        override def outputEncoder: Encoder[Int] = Encoders.scalaInt
      }
        .toColumn.name("countReviews")
      ).map(r => GroupInfo(r._1, r._2))
      .orderBy(desc("countReviews"))
      .limit(1)
//    ds
//      .groupByKey(key)
//      .mapGroups { (name, iter) =>
//        val numTotal = iter.map(r => r.NumReviews).sum
//        GroupInfo(name, numTotal)
//      }
//      .orderBy(desc("countReviews"))
//      .limit(1)
  }
  val companyDS = ds1
    .transform(findLeader(_.Company))

  case class NameLocInfo(
                        name: String,
                        location: String,
                        count: Int
                      )
  def join2Ds(ds2: Dataset[GroupInfo], joinCol: String)(ds1: Dataset[JobIndustryNum]) = {
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


  val companyLocDS = ds1
    .transform(join2Ds(companyDS, "Company"))
    .groupByKey(gr => (gr.Company, gr.Location))
    .mapGroups { (name, iter) =>
      val numTotal = Option(iter.map(r => r.NumReviews).sum).getOrElse(0)
      NameLocInfo(name._1, name._2, numTotal)
    }


  case class AggInfo(name: String,
                     count_max: Int,
                     count_min: Int
                    )

  val aggCompanyDS = companyLocDS
    .groupByKey(_.name)
    .mapGroups { (name, iter) =>
      val buffer = iter.map(r => r.count).toSeq
      val maxCount = Option(buffer.max).getOrElse(0)
      val minCount = Option(buffer.min).getOrElse(0)
      AggInfo(name, maxCount, minCount)
    }

  case class StatsInfo(name: String,
                       stats_type: String,
                       location: Array[String],
                       count: Int,
                       count_type: String
                      )

  val statsCompanyDS = aggCompanyDS.as("ds2")
    .joinWith(companyLocDS.as("ds1"),
      col("ds1.name") === col("ds2.name") &&
        (col("ds1.count") === col("ds2.count_min") ||
        col("ds1.count") === col("ds2.count_max")),
    "inner")
    .map(record => {
      val count_type = if (record._1.count_max == record._2.count) "max" else "min"
      StatsInfo(
        record._2.name,
        "company",
        Array(record._2.location),
        record._2.count,
        count_type
      )}
    )

  val jobDS = ds1
    .transform(findLeader(_.JobTitle))


  val jobLocDS = ds1
    .transform(join2Ds(jobDS, "JobTitle"))
    .groupByKey(gr => (gr.JobTitle, gr.Location))
    .mapGroups { (name, iter) =>
      val numTotal = Option(iter.map(r => r.NumReviews).sum).getOrElse(0)
      NameLocInfo(name._1, name._2, numTotal)
    }


  val aggJobDS = jobLocDS
    .groupByKey(_.name)
    .mapGroups { (name, iter) =>
      val buffer = iter.map(r => r.count).toSeq
      val maxCount = Option(buffer.max).getOrElse(0)
      val minCount = Option(buffer.min).getOrElse(0)
      AggInfo(name, maxCount, minCount)
    }

  val statsJobDS = aggJobDS.as("ds2")
    .joinWith(
      jobLocDS.as("ds1"),
      col("ds1.name") === col("ds2.name") &&
        (col("ds1.count") === col("ds2.count_min") ||
          col("ds1.count") === col("ds2.count_max")),
      "inner")
    .map(record => {
      val count_type = if (record._1.count_max == record._2.count) "max" else "min"
      StatsInfo(
        record._2.name,
        "job",
        Array(record._2.location),
        record._2.count,
        count_type
      )
    })

  val statsDS = statsJobDS
    .unionByName(statsCompanyDS)
    .groupByKey(rec => (rec.name, rec.stats_type, rec.count, rec.count_type))
    .mapGroups { (seq, iter) =>
      val collectCol =  iter.flatMap(_.location).toArray
      StatsInfo(
        seq._1,
        seq._2,
        collectCol,
        seq._3,
        seq._4
      )
    }


  statsDS.show(20, false)
  statsDF.show(20, false)
  spark.stop()

}
