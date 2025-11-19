package com.example

import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.functions._

object BroadcastJoin extends App {

  val spark = SparkSession.builder().appName("BroadcastJoin").master("local").getOrCreate()

  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

  def addColumn(df: DataFrame, n: Int): DataFrame = {
    val columns = (1 to n).map(col => s"col_$col")
    columns.foldLeft(df)((df, column) => df.withColumn(column, lit("n/a")))
  }

  val data1 = (1 to 500000).map(i => (i, i * 100))
  val data2 = (1 to 10000).map(i => (i, i * 1000))
  import spark.implicits._
  val repartitionedById1 = data1.toDF("id","salary").repartition(col("id"))
  val repartitionedById2 = data2.toDF("id","salary").repartition(col("id"))

  val joinedDF2 = repartitionedById2.join(repartitionedById1, "id")
  val dfWithColumns2 = addColumn(joinedDF2, 20)
  dfWithColumns2.collect()
  dfWithColumns2.explain()

  val df1WithColumns = addColumn(repartitionedById1, 20)
  val joinedColumnsDF2 = repartitionedById2.join(df1WithColumns, "id")
  joinedColumnsDF2.collect()
  joinedColumnsDF2.explain()
  System.in.read()
  spark.stop()


}