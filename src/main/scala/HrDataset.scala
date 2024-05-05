import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, Encoders}

object HrDataset extends App with Context {
  override val appName: String = "3_4_PracticeDS_2"

  import spark.implicits._

  case class Position(
                 position_id: Int,
                 position: String
               )

  val hrDS:  Dataset[Position] = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/hrdataset.csv")
      .select(col("PositionID").as("position_id"),
        col("Position").as("position")
      )
      .as[Position]


  val request = List("BI", "it") // scala.io.StdIn.readLine().split(",")


  def findPosition(request: Seq[String])(ds: Dataset[Position]): Dataset[Position]= {
    ds
      .filter(row => {
        request
          .map(l => l.toLowerCase())
          .exists(req => row.position.toLowerCase().startsWith(req))
      })
      .distinct()
  }

  val hrResDS = hrDS
    .transform(findPosition(request))


  hrResDS.show()

  spark.stop()

}
