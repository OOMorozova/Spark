import org.apache.spark.sql.types._
import org.apache.spark.sql.SaveMode
//Напишите код, который
// считывает файл
// принтит схему файла - создание схемы является одной из ваших задач
// сохраняет файл в формате .parquet:
  // в качестве разделителя для значений используется запятая
  // первая строчка в файле содержит названия столбцов
  // если файл уже существует в указанной директории, то существующий файл должен быть перезаписан

object ReadWrite extends App with Context {
  override val appName: String = "2_2_ReadWrite"
  val moviesSchema = StructType(Seq(
    StructField("", IntegerType),
    StructField("show_id", StringType),
    StructField("type", StringType),
    StructField("title", StringType),
    StructField("director", StringType),
    StructField("cast", StringType),
    StructField("country", StringType),
    StructField("date_added", TimestampType),
    StructField("release_year", StringType),
    StructField("rating", StringType),
    StructField("duration", IntegerType),
    StructField("listed_in", StringType),
    StructField("description", StringType),
    StructField("year_added", IntegerType),
    StructField("month_added", DoubleType),
    StructField("season_count", IntegerType)
  ))

  val moviesDF = spark.read
    .format("csv")
    .option("header", "true")
    .schema(moviesSchema)
    .load("src/main/resources/movies_on_netflix.csv")

  moviesDF.show(5,false)

  moviesDF.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data")

//  moviesDF.dtypes
//    .foreach(x => println(s"StructField(\"${x._1}\", ${x._2}),"))

  spark.stop()


}
