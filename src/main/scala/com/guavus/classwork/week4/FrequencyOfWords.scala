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

//    val listOfWords = List("big", "Data", "Is", "SUPER", "Interesting", "BIG", "data", "is", "A", "Trending", "Technology")
//
//    val words_rdd = spark.sparkContext.parallelize(listOfWords);
//
//    val normalized_words = words_rdd.map(word => word.toLowerCase)
//
//    val wordMap = normalized_words.map(word => (word, 1))
//
//    val words_freq = wordMap.reduceByKey((a, b) => a + b)
//
//    words_freq.foreach(println)

    val anotherWordCount = spark.sparkContext.textFile("src/main/resources/wordcount.txt")
    println(anotherWordCount.count())
//    val flattendRDD = anotherWordCount.flatMap(x => x.split("\\W+"))
//    val normalizedWords = flattendRDD.map(word => word.toLowerCase)
//
//    val wordsToFilter = Set("a", "an", "then", "the")
//    val filteredWords = normalizedWords.filter(x => !wordsToFilter.contains(x.toLowerCase())).filter(x => x.length > 3)
//    val wordMapNew = filteredWords.map(word => (word, 1))
//    val wordsFreq = wordMapNew.reduceByKey((a, b) => a + b).sortBy(x => x._2, ascending = false)
//    wordsFreq.collect().foreach(x => println(x))

  }
}
