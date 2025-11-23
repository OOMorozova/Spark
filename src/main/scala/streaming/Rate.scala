package streaming

import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.Dataset

import java.sql.{Timestamp}

// для каждых десяти элементов value требуется выводить их среднее значение

object Rate extends App with Context {
  override val appName: String = "3_3_1_Rate"

  import spark.implicits._

  val streamDf = spark
    .readStream
    .format("rate")
    .option("rowsPerSecond", 3)
    .load

  case class Rate(timestamp: Timestamp,
                      value: Long)

  case class StateRate(groupId: Int,
                       valueCount: Int,
                       valueSum: Double
                        )

  case class RateAvg(groupId: Int,
                     valueCount: Int,
                     valueSum: Double,
                     avgValue: Double
                    )

  def calculateRateAvg(ds: Dataset[Rate]) = {
    ds
      .withWatermark("timestamp", "1 seconds")
      .groupByKey(x => (x.value / 10).asInstanceOf[Int])
    .flatMapGroupsWithState(outputMode=OutputMode.Update(),
      GroupStateTimeout.EventTimeTimeout()
    )((key, values, state: GroupState[StateRate]) => {
      val currentState = {
        if (state.exists) state.get
        else StateRate(key, 0, 0.0)
      }

      var newSum = currentState.valueSum
      var newCount = currentState.valueCount

      for (value <- values) {
        newSum += value.value
        newCount += 1
      }

      state.update(new StateRate(key, newCount, newSum))
      Iterator(RateAvg(key, newCount, newSum, newSum / newCount))

    }
    )
  }

  val streamDs = streamDf.as[Rate]
  val transformedDf =streamDs
    .transform(calculateRateAvg)

  val writeQuery = transformedDf.writeStream
    .format("console")
    .outputMode(OutputMode.Update())
    .start()

  val seconds = 100

  writeQuery.awaitTermination(1000 * seconds)

  spark.stop()
}