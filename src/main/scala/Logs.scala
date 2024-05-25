import org.json4s.DefaultFormats.dateFormat

import java.time.Instant
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale}


object Logs extends App with Context {
  override val appName: String = "4_2_RDD_1"

  val sc = spark.sparkContext
  case class Log(
                      id: Int,
                      host: String,
                      time: Date,
                      response: Int,
                      bytes: Int)

  val csvFile = sc.textFile("src/main/resources/logs_data.csv")
  val logsRDD = csvFile
    .map(line => line.split(",(?=(?:[^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)"))
//    .map(line => line.split(","))
    .filter(values => values(0) != "" && values.length == 7)
    .map(values =>
        Log(values(0).toInt,
             values(1),
             Date.from(Instant.ofEpochSecond(values(2).toLong)),
             values(5).toInt,
             values(6).toInt)
    )


  val goodRows  = logsRDD.count()

  // отобразить статистику по успешно считанным строчкам и неуспешно
  println(s"success rows:  ${goodRows}, unsuccess rows: ${ csvFile.count() - goodRows - 1}")

  // найти количество записей для каждого кода ответа (response)
  val responseRDD =
    logsRDD
      .map(rec => (rec.response, 1))
      .reduceByKey(_ + _)

  responseRDD.foreach(println)

  // следует собрать статистику по размеру ответа (bytes): сколько всего байт было отправлено
  // (сумма всех значений), среднее значение, максимальное и минимальное значение.
  val bytesRDD = logsRDD
    .map(_.bytes)

  val sumBytes = bytesRDD
    .sum()

  val maxBytes = bytesRDD
    .max()

  val minBytes = bytesRDD
    .min()

  val avgBytes = sumBytes / bytesRDD.count()

  println(s"sum:  ${sumBytes}, min:  ${minBytes}, max:  ${maxBytes}, avg:  ${avgBytes}")

//  4. подсчитать количество уникальных хостов(host), для которых представлена статистика
  val hosts = logsRDD
    .map(_.host)
    .distinct()
    .count()

  println(s"Количество уникальных хостов:  ${hosts}")

  //  5. найти топ - 3 наиболее часто встречающихся хостов(host).Результат анализа, выводимый на экран,
  //  должен включать в себя хоста и подсчитанную частоту встречаемости этого хоста.

  implicit val strInt: Ordering[(String, Int)] =
    Ordering.fromLessThan[(String, Int)]((sa: (String, Int), sb: (String, Int)) => sa._2 < sb._2)

  val topHosts = logsRDD
    .map(rec => (rec.host, 1))
    .reduceByKey(_ + _)
    .top(3)

  topHosts.foreach(println)

  //  6. определите дни недели (понедельник, вторник и тд), когда чаще всего выдавался ответ (response) 404.
  //  В статистике отобразите день недели (в каком виде отображать день остается на ваше усмотрение,
  //  например, Mon, Tue ) и сколько раз был получен ответ 404. Достаточно отобразить топ-3 дней.

  val dowText = new SimpleDateFormat("E", Locale.US)
  val stat404 = logsRDD
    .filter(rec => rec.response==404)
    .map(rec => (dowText.format(rec.time), 1))
    .reduceByKey(_ + _)
    .top(3)

  stat404.foreach(println)

//  вариант с объединением решения для нахождения ответов на вопрос 2 и 6
   case class ResponseCount(
                          response: Int,
                          responseCount: Int,
                          days: Map[String, Int] = Map()
                        )

  val targetResponse = 404

  val responseCountRdd =
    logsRDD
      .groupBy(_.response)
      .map {
        case (response, logs) if response == targetResponse => ResponseCount(
          response,
          logs.size,
          logs
            .groupBy(rec => {
              dowText.format(rec.time)
            })
            .map {
              case (day, logs) => (day, logs.size)
            })
        case (response, logs) => ResponseCount(response, logs.size)
      }

  responseCountRdd.foreach(println)

  spark.stop()

}
