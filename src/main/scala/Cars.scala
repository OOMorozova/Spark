import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoders}
import java.time._
import java.time.format._
import java.util.Locale

object Cars extends App with Context {
  override val appName: String = "3_4_PracticeDS_1"

  import spark.implicits._

  case class Car(
                   id: Integer,
                   price: Integer,
                   brand: String,
                   car_type: String,
                   mileage: Option[Double],
                   color: String,
                   date_of_purchase: String
                 )

  val carsSchema = Encoders.product[Car].schema
  val carsDS:  Dataset[Car] = spark.read.format("csv")
      .option("header", "true")
      .schema(carsSchema)
      .csv("src/main/resources/cars.csv")
      .as[Car]

  case class CarRes(
                   id: Integer,
                   price: Integer,
                   brand: String,
                   car_type: String,
                   mileage: Double,
                   color: String,
                   date_of_purchase: String,
                   avg_mileage: Double,
                   years_since_purchase: Int
                 )

  def countAge(purchaseDate: String, currentDate: LocalDate): Int = {
    val dateStr = purchaseDate.replaceAll("\\s", "-")
    val formatDate1 = """\d{4}-\d{2}-\d{2}""".r
    val formatter =
      if (formatDate1.matches(dateStr))
        DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.ENGLISH)
      else DateTimeFormatter.ofPattern("yyyy-MMM-dd", Locale.ENGLISH)
    val datePurchaseFormat = LocalDate.parse(dateStr, formatter)
    Period.between(datePurchaseFormat, currentDate).getYears
  }

  def yearSince(avgMileage: Double, currentDate: LocalDate)(ds: Dataset[Car]): Dataset[CarRes] = {
    ds.map { line =>
      CarRes(line.id,
        line.price,
        line.brand,
        line.car_type,
        line.mileage.getOrElse(0.0),
        line.color,
        line.date_of_purchase,
        avgMileage,
        countAge(line.date_of_purchase, currentDate)
      )
    }
  }

  def countAvgMileage(ds: Dataset[Car]): Double = {
    val totalMileage: (Double, Int) = ds
      .map(el => (el.mileage.getOrElse(0.0), 1))
      .reduce((a, b) => (a._1 + b._1, a._2 + b._2))

    totalMileage._1 / totalMileage._2
  }


  val avgMileage: Double  = countAvgMileage(carsDS)

  val resDS = carsDS
    .transform(yearSince(avgMileage, LocalDate.now()))

//  avg_mileage (в колонке во всех строчках будет записано одно и то же число - средний пробег всех имеющихся машин; если у какой-либо машины не указан пробег, считайте, что он равен 0)
//  years_since_purchase(колонка хранит информацию о том, сколько полных лет прошло со дня покупки, значение должно быть рассчитано для каждой машины)
  resDS.show(13,false)
  spark.stop()

}
