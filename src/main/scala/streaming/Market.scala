package streaming

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{Dataset, Encoders}

import java.sql.Timestamp

// для каждого клиента (customerId) выводящую на экран информацию об общем количестве товаров в корзине и общей сумме товаров.

object Market extends App with Context {
  override val appName: String = "3_3_3_Market"

  import spark.implicits._

  case class Item(ProductId: String,
                  Price: Double)

  case class EventType(
                        AddToCart: Option[Item],
                        RemoveFromCart: Option[Item],
                        Purchase: Option[Item],
                        PageView: Option[Item]
                      )
  case class Log(timestamp: String,
                 customerId: String,
                 eventType: EventType
                 )

  case class FlatLog(timestamp: Timestamp,
                     customerId: String,
                     event: String,
                     productId: String,
                     price: Double
                )

  case class StateCart(customerId: String,
                       totalProducts: Integer,
                       totalPrice: Double
                    )


  val logSchema = Encoders.product[Log].schema

  val streamDs = spark.readStream
    .schema(logSchema)
    .json("src/main/resources/events")
    .select(
      to_timestamp(col("timestamp"), "dd:MM:yyyy H:mm:ss").alias("timestamp"),
      col("customerId"),
      when(col("EventType.AddToCart").isNotNull, "AddToCart")
        .when(col("EventType.RemoveFromCart").isNotNull, "RemoveFromCart")
        .when(col("EventType.Purchase").isNotNull, "Purchase")
        .when(col("EventType.PageView").isNotNull, "PageView")
        .alias("event"),
      coalesce(
        col("EventType.AddToCart.ProductId"),
        col("EventType.RemoveFromCart.ProductId"),
        col("EventType.Purchase.ProductId"),
        col("EventType.PageView.ProductId")
      ).alias("productId"),
      coalesce(
        col("EventType.AddToCart.Price"),
        col("EventType.RemoveFromCart.Price"),
        col("EventType.Purchase.Price"),
        col("EventType.PageView.Price")
      ).alias("price")
    )
    .filter(col("event").isin("RemoveFromCart", "AddToCart", "Purchase"))
    .as[FlatLog]

  def processRec(
                  key: String,
                  values: Iterator[FlatLog],
                  state: GroupState[StateCart]): StateCart = {
    val empty =  StateCart(key, 0, 0.0)
    if (state.hasTimedOut) {
      state.remove()
      empty
    } else {
      val currentState = {
        if (state.exists) state.get
        else empty
      }
      val (totalPrice, totalProducts) = values.foldLeft((currentState.totalPrice, currentState.totalProducts))((accumulator, element) => {
        element.event match {
          case "AddToCart" => (accumulator._1 + element.price, accumulator._2 + 1)
          case "RemoveFromCart" | "Purchase" => (accumulator._1 - element.price, accumulator._2 - 1)
          case _ => accumulator
        }

      })

      state.update(StateCart(key, totalProducts, totalPrice))
      state.setTimeoutDuration("30 minutes")
      StateCart(key, totalProducts, totalPrice)
    }
  }

  def calculatePrice(ds: Dataset[FlatLog]) = {
    ds
      .groupByKey(x => x.customerId)
      .mapGroupsWithState(GroupStateTimeout.ProcessingTimeTimeout()
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