
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


object Transactions extends App with Context {
  override val appName: String = "6_5_Optimize_2"
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

  /*
  1)фильтрация как можно раньше Predicate Pushdown
  2)уменьшение количества колонок при чтении Projection Pushdown
  3) broadcast
   */

  def readCSV(path: String) = {
    spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(path)

  }

  val tranDF = readCSV("src/main/resources/transactions.csv")

  val codesDF = readCSV("src/main/resources/country_codes.csv")

  def join2Df(df1: DataFrame, colCond: String)(df2: DataFrame): DataFrame = {
    val joinCondition =  df2.col(colCond) === df1.col(colCond)
    df2.as("df2")
      .join(df1.as("df1"), joinCondition, "inner")

  }

  def filterEqual(fCol: String, fLit: String)(df: DataFrame): DataFrame = {
    val isEqual = (col(fCol) === lit(fLit))
    df.filter(isEqual)

  }

  def filterGrande(fCol: String, fLit: Integer)(df: DataFrame): DataFrame = {
    val isGrande = (col(fCol) > lit(fLit))
    df.filter(isGrande)

  }

 def selectCols(df: DataFrame): DataFrame = {
     df.select("TransactionNo","ProductName", "Quantity", "CountryCode")
  }

  def extractLead(groupCol: String)(df: DataFrame): DataFrame = {
    df.groupBy(col(groupCol))
      .agg(sum("Quantity").as("Quantity"))
      .orderBy(desc("Quantity"))
      .select(groupCol)
      .limit(1)
  }


  def extractAvg(groupCol: String)(df: DataFrame): Double = {
    val countTran = df.select(groupCol).dropDuplicates().count().toDouble
    val quantityProd = df.agg(sum("Quantity")).collect().head.getLong(0).toDouble
    quantityProd / countTran

  }

  val CodeUkDF =
    codesDF
      .transform(filterEqual("Country", "United Kingdom"))

  val bCodeUkDF = broadcast(CodeUkDF)

  val countryDF =
    tranDF
      .transform(filterEqual("Date", "2018-12-01"))
      .transform(filterGrande("Quantity", 0))
      .transform(selectCols)
      .transform(join2Df(bCodeUkDF, "CountryCode"))

  //наиболее часто покупаемый товар (ProductName)
  val pupularProdDF =
    countryDF
      .transform(extractLead("ProductName"))


  println(s"Top ProductName DF: ${pupularProdDF.collect().head.getString(0)}")

  //сколько товаров в среднем посетители приобретают за одну транзакцию
  val avgQuantity = extractAvg("TransactionNo")(countryDF)
  println(s"Товаров в среднем посетители приобретают за одну транзакцию DF: ${avgQuantity}")


  //RDD
  val sc = spark.sparkContext

  case class Transaction(transaction_no: String,
                         date: String,
                         product_name: String,
                         quantity: Integer,
                         code: String)


  val codesFile = sc.textFile("src/main/resources/country_codes.csv")
  val codesVal = codesFile
    .map(line => line.split(","))
    .filter(values => values(0) == "United Kingdom")
    .map(values => values(1)).collect().head

  val bCodesArray = sc.broadcast(codesVal)


  val tranFile = sc.textFile("src/main/resources/transactions.csv")
  val tranRDD = tranFile
    .map(line => line.split(","))
    .filter(values => values(0) != "TransactionNo")
    .filter(values => values(1) == "2018-12-01")
    .filter(values => values(5).toInt > 0)
    .filter(values => values(7) == bCodesArray.value)
    .map(values =>
      Transaction(values(0),
        values(1),
        values(3),
        values(5).toInt,
        values(7))
    )

  //наиболее часто покупаемый товар (ProductName)
  implicit val strInt: Ordering[(String, Integer)] =
    Ordering.fromLessThan[(String, Integer)]((sa: (String, Integer), sb: (String, Integer)) => sa._2 < sb._2)

  val topProduct =
    tranRDD
      .map(rec => (rec.product_name, rec.quantity))
      .reduceByKey(_ + _)
      .top(1)
      .map(rec => rec._1)

  print("Top ProductName RDD: ")
  topProduct.foreach(println)


  //сколько товаров в среднем посетители приобретают за одну транзакцию

  val transCount =
    tranRDD
      .map(rec => rec.transaction_no)
      .distinct()
      .count()
  val quantityProds =
    tranRDD
      .map(rec => rec.quantity.intValue())
      .sum()

  val avgProds = quantityProds/transCount

  println(s"Товаров в среднем посетители приобретают за одну транзакцию RDD: ${avgProds}")


//  System.in.read()

  spark.stop()

}
