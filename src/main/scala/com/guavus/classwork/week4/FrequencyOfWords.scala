package com.guavus.classwork.week4

import org.apache.spark.sql.SparkSession

object FrequencyOfWords {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("frequencyOfWords")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val listOfWords = List("big", "Data", "Is", "SUPER", "Interesting", "BIG", "data", "is", "A", "Trending", "Technology")

    val words_rdd = spark.sparkContext.parallelize(listOfWords);

    val normalized_words = words_rdd.map(word => word.toLowerCase)

    val wordMap = normalized_words.map(word => (word, 1))

    val words_freq = wordMap.reduceByKey((a, b) => a + b)

    words_freq.foreach(println)

  }
}
