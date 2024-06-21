import io.circe._
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.parser._


object Amazon extends App with Context {
  override val appName: String = "4_3_RDD"

  val sc = spark.sparkContext

  case class ProductOption(
                       uniq_id: Option[String],
                       product_name: Option[String],
                       manufacturer: Option[String],
                       price: Option[String],
                       number_available: Option[String],
                       number_of_reviews: Option[Int])


  case class Product(
                       uniq_id: String,
                       product_name: String,
                       manufacturer: String,
                       price: String,
                       number_available: String,
                       number_of_reviews: Int)


//  implicit val jsonEncoder: Encoder[ProductOption] = deriveEncoder[ProductOption]
  implicit val jsonDecoder: Decoder[ProductOption] = deriveDecoder[ProductOption]


  val parsedRDD =
    sc.textFile("src/main/resources/amazon_products.json")
      .map(rec => decode[ProductOption](rec) match {
        case Left(failure) => println("Invalid JSON :(")
        case Right(decodedJson) =>
          Product(
            decodedJson.uniq_id.orNull,
            decodedJson.product_name.orNull,
            decodedJson.manufacturer.orNull,
            decodedJson.price.orNull,
            decodedJson.number_available.orNull,
            decodedJson.number_of_reviews.getOrElse(0)
          )
      })


  parsedRDD.foreach(println)

  spark.stop()

}
