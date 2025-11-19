package org.example

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoder, Encoders, SparkSession}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.StructType

object test2 extends App {
  case class Pizza(id: Integer,
                   order_id: Integer,
                   date: String,
                   user_id: Integer,
                   order_type: String,
                   value: Double,
                   address_id: Integer
                  )

  case class AddrCount(order_type: String,
                       address_id: Integer,
                       orders_cnt: Integer
                      )

  case class MaxOrders(order_type: String,
                       orders_max: Integer
                      )

  case class OrdersTotal(order_type: String,
                         orders_total: Integer
                        )

  case class Stat(order_type: String,
                  orders_total: Integer,
                  address_id: Integer,
                  orders_cnt: Integer
                 )

  val spark = SparkSession.builder()
    .appName("Playground")
    .master("local")
    .getOrCreate()
  val sc = spark.sparkContext

  import spark.implicits._

  val pizzaSchema: StructType = Encoders.product[Pizza].schema

  val pizzaDS: Dataset[Pizza] = spark.read
    .option("header", "true")
    .schema(pizzaSchema)
    //      .csv(args(0))
    .csv("spark-cluster/spark-data/pizza_orders.csv")
    .as[Pizza]

  def aggOrders(ds: Dataset[Pizza]): Dataset[OrdersTotal] = {
    val ordersCounter = new Aggregator[Pizza, Int, Int] {

      override def zero: Int = 0

      override def reduce(b: Int, a: Pizza): Int = b + 1

      override def merge(b1: Int, b2: Int): Int = b1 + b2

      override def finish(reduction: Int): Int = reduction

      override def bufferEncoder: Encoder[Int] = Encoders.scalaInt

      override def outputEncoder: Encoder[Int] = Encoders.scalaInt
    }
      .toColumn.name("orders_total")


    ds.groupByKey(rec => rec.order_type)
      .agg(ordersCounter)
      .map(r => OrdersTotal(r._1, r._2))
  }

  def aggAddr(ds: Dataset[Pizza]): Dataset[AddrCount] = {
    val ordersCounter = new Aggregator[Pizza, Int, Int] {

      override def zero: Int = 0

      override def reduce(b: Int, a: Pizza): Int = b + 1

      override def merge(b1: Int, b2: Int): Int = b1 + b2

      override def finish(reduction: Int): Int = reduction

      override def bufferEncoder: Encoder[Int] = Encoders.scalaInt

      override def outputEncoder: Encoder[Int] = Encoders.scalaInt
    }
      .toColumn.name("orders_by_addr")


    ds.groupByKey(rec => (rec.order_type, rec.address_id))
      .agg(ordersCounter)
      .map(r => AddrCount(r._1._1, r._1._2, r._2))
  }

  def findMaxCount(ds: Dataset[AddrCount]): Dataset[MaxOrders] = {
    val orders_max =
      new Aggregator[AddrCount, Int, Int] {
        override def zero: Int = 0

        override def reduce(b: Int, a: AddrCount): Int = if (a.orders_cnt > b) a.orders_cnt else b

        override def merge(b1: Int, b2: Int): Int = if (b1 > b2) b1 else b2

        override def finish(reduction: Int): Int = reduction

        override def bufferEncoder: Encoder[Int] = Encoders.scalaInt

        override def outputEncoder: Encoder[Int] = Encoders.scalaInt
      }
        .toColumn.name("orders_max")

    ds.groupByKey(rec => (rec.order_type))
      .agg(orders_max)
      .map(r => MaxOrders(r._1, r._2))
  }

  def joinAddrsDs(ds2: Dataset[MaxOrders])(ds1: Dataset[AddrCount]): Dataset[AddrCount] = {
    ds1.joinWith(
        ds2,
        ds1.col("order_type") === ds2.col("order_type") &&
          ds1.col("orders_cnt") === ds2.col("orders_max"),
        "inner"
      )
      .map(rec =>
        AddrCount(
          rec._1.order_type,
          rec._1.address_id,
          rec._1.orders_cnt
        )
      )

  }

  def joinOrdersDs(ds2: Dataset[AddrCount])(ds1: Dataset[OrdersTotal]): Dataset[Stat] = {
    ds1.joinWith(
        ds2,
        ds1.col("order_type") === ds2.col("order_type"),
        "left"
      )
      .map(rec =>
        Stat(
          rec._1.order_type,
          rec._1.orders_total,
          rec._2.address_id,
          rec._2.orders_cnt
        )
      )

  }

  val addrDS = pizzaDS
    .transform(aggAddr)

  val maxAddrDS = addrDS
    .transform(findMaxCount)

  val justMaxAddr = addrDS
    .transform(joinAddrsDs(maxAddrDS))

  val statOrders = pizzaDS
    .transform(aggOrders)
    .transform(joinOrdersDs(justMaxAddr))

  addrDS.sort(col("orders_cnt").desc).show(10, false)
  statOrders.show(10, false)
  spark.stop()

}
