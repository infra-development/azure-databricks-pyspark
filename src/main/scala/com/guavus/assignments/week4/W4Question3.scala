package com.guavus.assignments.week4

import com.guavus.utility.AppConfig
import org.apache.spark.sql.SparkSession

object W4Question3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Week4 Question3")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    val review_rdd = spark.sparkContext.textFile(AppConfig.student_review_path)

    // 1.Find the top 20 words from Trendy tech Students Google Reviews excluding the boring words.
    val words = review_rdd.flatMap(x => x.split(" ")).map(x => x.toLowerCase())
    val filtered_words = words.filter(x => x.length > 3).filter(x => !Set("this").contains(x.toLowerCase()))
    filtered_words.map(x => (x, 1))
      .reduceByKey((x, y) => x + y)
      .sortBy(x => x._2, ascending = false)
      .take(20)
      .foreach(println)


  }

}
