import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Freelancers extends App {
  val spark = SparkSession
    .builder()
    .appName("Freelancers")
    .master("local[*]")
    .getOrCreate()

  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

  def readCSV(path: String) = {
    spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(path)

  }
  val freelancersDF = readCSV("src/main/resources/freelancers.csv")

  val offersDF = readCSV("src/main/resources/offers.csv")


  val badDF = freelancersDF
    .join(offersDF, Seq("category", "city"))
    .filter(abs(freelancersDF.col("experienceLevel") - offersDF.col("experienceLevel")) <= 1)
    .groupBy("id")
    .agg(avg("price").as("avgPrice"))

  badDF.collect()

  val skewFactor = spark.conf.get("spark.sql.shuffle.partitions").toInt - 1
  val saltDF = spark.range(skewFactor).toDF("salt")


  def join2Df(df1: DataFrame, joinCondition: Seq[String])(df2: DataFrame): DataFrame = {
    df2.as("df2")
      .join(df1.as("df1"), joinCondition)
  }

  def filterLevelDF(df: DataFrame): DataFrame = {
    val condition = abs(freelancersDF.col("experienceLevel") - saltedOffersDF.col("experienceLevel")) <= 1
    df.filter(condition)
  }

  def aggAvg(df: DataFrame) = {

    df.groupBy("id")
      .agg(avg("price").as("avgPrice"))
  }
  def withSalt1(df: DataFrame)= {
    df.crossJoin(saltDF)
  }

  def withSalt2(df: DataFrame) = {
    df.withColumn("salt", (lit(skewFactor) * rand()).cast("int"))
  }

  def withSaltedCol(df: DataFrame) = {
    df.withColumn("salted_col",
        concat(col("category"), lit("_"), col("city"), lit("_"), col("salt")))
      .drop("salt")
  }

  val saltedOffersDF = offersDF
    .transform(withSalt2)
    .transform(withSaltedCol)


  val goodDF = freelancersDF
    .transform(withSalt1)
    .transform(withSaltedCol)
    .transform(join2Df(saltedOffersDF, Seq("salted_col")))
    .transform(filterLevelDF)
    .transform(aggAvg)

  goodDF.collect()

  goodDF.show()


  System.in.read()
  spark.stop()


}