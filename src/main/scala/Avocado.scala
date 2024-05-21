import scala.io.Source


object Avocado extends App with Context {
  override val appName: String = "4_2_RDD_1"

  val sc = spark.sparkContext
  case class Avocado(
                      id: Int,
                      date: String,
                      avgPrice: Double,
                      volume: Double,
                      year: String,
                      region: String)


  def readAvocados(filename: String) =
    Source.fromFile(filename)
      .getLines()
      .drop(1)
      .map(line => line.split(","))
      .map(values => Avocado(
        values(0).toInt,
        Option(values(1)).getOrElse("1999-01-01"),
        values(2).toDouble,
        values(3).toDouble,
        values(12),
        values(13)
      )).toList

  val avocadoRDD = sc.parallelize(readAvocados("src/main/resources/avocado.csv"))

  // подсчитайте количество уникальных регионов (region), для которых представлена статистика
  val uniqCountRegions =
    avocadoRDD
      .map(v => v.region)
      .distinct()
      .count()

  println(uniqCountRegions)

  // выберите и отобразите на экране все записи о продажах авокадо, сделанные после 2018-02-11
  val salesAfterTargetDateRdd = avocadoRDD
    .filter(v => v.date > "2018-02-11")

  salesAfterTargetDateRdd.foreach(println)

  // найдите месяц, который чаще всего представлен в статистике
  val popularMonth = avocadoRDD
    .filter(rec => rec.date > "1999-01-01")
    .map(rec => (rec.date.split("-")(1), 1))
    .reduceByKey(_ + _)
    .max()

  println(popularMonth)

  //найдите максимальное и минимальное значение avgPrice
  val avgPriceRDD = avocadoRDD
    .map(_.avgPrice)

  val maxAvgPrice = avgPriceRDD
    .max()

  val minAvgPrice = avgPriceRDD
    .min()

  println(maxAvgPrice, minAvgPrice)

  // отобразите средний объем продаж (volume) для каждого региона (region)
  case class regionVolume(
                           region: String,
                           volume: Double)

  val avgVolumeByRegionRDD = avocadoRDD
    .groupBy(_.region)
    .mapValues(x => x.toList.map(_.volume).sum / x.toList.map(_.volume).length)
    .map(x => regionVolume(x._1, x._2))

  avgVolumeByRegionRDD.foreach(println)

  spark.stop()

}
