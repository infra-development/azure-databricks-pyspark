package com.guavus.assignments.week6

import com.guavus.utility.AppConfig
import org.apache.spark.sql.SparkSession

object W6Question3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Week6 Question3")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    val train_path = AppConfig.train_path

    val df = spark.read.options(Map("header" -> "true", "inferSchema" -> "true")).csv(train_path)
    df.show(5, truncate = false)
    df.printSchema()

    val new_df = df.drop("passenger_name", "age")
    new_df.show(5)

    val duplicate_removed = df.dropDuplicates(Array("train_number", "ticket_number"))
    println("Trains after removing duplicates : "+duplicate_removed.count())

    val unique_trains = df.select("train_name").distinct()
    println("Unique trains : "+ unique_trains.count())

  }
}
