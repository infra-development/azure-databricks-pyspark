package com.guavus

import org.apache.spark.sql.SparkSession

object SampleScala extends App {

  val spark = SparkSession.builder()
    .appName("SampleScalaApp")
    .master("local[*]")
    .getOrCreate()

  val df = spark.read.csv("D:/files/orders/part-00000")

  val rdd_order = spark.sparkContext.textFile("D:/files/orders/part-00000")
  rdd_order.take(10).foreach(println)

}
