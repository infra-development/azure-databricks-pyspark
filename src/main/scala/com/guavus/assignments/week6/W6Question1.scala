package com.guavus.assignments.week6

import org.apache.spark.sql.SparkSession

object W6Question1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Week6 Question1")
      .master("local[*]")
      .getOrCreate()

    val list = Array(
      ("Spring", 12.3),
      ("Summer", 10.5),
      ("Autumn", 8.2),
      ("Winter", 15.1)
    )

    val df = spark.createDataFrame(list).toDF("season", "windspeed")
    df.show()

  }
}
