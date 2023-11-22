import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
object playground extends App {
   val spark=SparkSession.builder()
        .appName("Playground")
        .master("local")
        .getOrCreate()
   val sc = spark.sparkContext

  //iris
  val iris = spark.read
    .format("json")
    .option("inferSchema", "true")
//    .option("multiline", "true")
    .load("src/main/resources/iris.json")
  iris.show(2)
  iris.printSchema()

  val irisArray: Array[Row] = iris.take(1)
  irisArray.foreach(println)

//test
  import spark.implicits._
   val courses=Seq(
        ("Scala",22),
        ("Spark",30)
        )

  val coursesDF=courses.toDF("title","duration (h)")
  coursesDF.show()
  spark.stop()

}
