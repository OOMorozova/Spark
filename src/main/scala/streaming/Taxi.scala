package streaming
import org.apache.spark.sql.{Dataset, Encoders}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.time.LocalDate

// для каждого клиента требуется выводить информацию о полной стоимости каждой его поездки на такси (fare + tips)
// при этом учитывайте, что каждая третья поездка в месяц идет со скидкой в 10%, но только если все три раза клиент
// расплачивается картой (в payment_type указано CreditCard)

object Taxi extends App with Context {
  override val appName: String = "3_3_2_Taxi"

  case class LogTaxi( taxi_id: Integer,
                   trip_start_timestamp: String,
                   trip_end_timestamp: String,
                   fare: Double,
                   payment_type: String,
                   customer_id: Integer,
                   tips: Double,
                   trip_date: LocalDate,
                   year_month: Long
                 )

  case class Taxi(fare: Double,
                  payment_type: String,
                  customer_id: Integer,
                  tips: Double,
                  trip_date: LocalDate,
                  year_month: Long
                 )

  case class StateTaxi(customerId: Int,
                       totalTrips: Integer,
                       creditStreak: Integer,
                       yearMonth: Long
                      )

  case class TaxiResult(customerId: Int,
                        tripDate: LocalDate,
                        tripCost: Double
                      )

  import spark.implicits._

  val taxiSchema = Encoders.product[LogTaxi].schema

  val streamDs = spark.readStream
    .format("csv")
    .option("header", "true")
    .schema(taxiSchema)
    .load("src/main/resources/taxi")
    .select(col("fare"),
      col("payment_type"),
      col("customer_id"),
      col("tips"),
      date_trunc("day", to_date(col("trip_start_timestamp"), "yyyy-M-dd HH:mm:ss")).cast(DateType).alias("trip_date"),
      date_trunc("mon", to_date(col("trip_start_timestamp"), "yyyy-M-dd HH:mm:ss")).cast(TimestampType).alias("year_month")
    ).as[Taxi]


  def processRec(
                  key: (Integer, Long),
                  values: Iterator[Taxi],
                  state: GroupState[StateTaxi]): Iterator[TaxiResult] = {
    val out = values.map(rec => {
      val currentState = {
        if (state.exists) state.get
        else StateTaxi(key._1, 0, 0, key._2)
      }

      val tripCount = currentState.totalTrips + 1

      val cardUsageCount: Integer =
        if (rec.payment_type == "CreditCard") currentState.creditStreak + 1
        else currentState.creditStreak

      val taxiPrice =
        if (tripCount % 3 == 0 & cardUsageCount >= 3) (rec.fare + rec.tips) * 0.9
        else rec.fare + rec.tips

      state.update(StateTaxi(key._1, tripCount, cardUsageCount, key._2))
      TaxiResult(key._1, rec.trip_date, taxiPrice)
    })

    out
  }

  def calculatePrice(ds: Dataset[Taxi]) = {
    ds
      .withWatermark("year_month", "1 day")
      .groupByKey(x => (x.customer_id, x.year_month))
      .flatMapGroupsWithState(outputMode=OutputMode.Update(),
        GroupStateTimeout.EventTimeTimeout()
      )(processRec)
  }

  val clientDs = streamDs
    .transform(calculatePrice)


  val writeQuery = clientDs.writeStream
    .format("console")
    .option("truncate", "false")
    .outputMode(OutputMode.Update())
    .start()

  writeQuery.awaitTermination()

  spark.stop()
}