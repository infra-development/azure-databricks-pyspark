package com.guavus.assignments.week6

import com.guavus.utility.AppConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}

object W6Question2 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Week6 Question1")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val library_data = AppConfig.library_path

    val schema = StructType(Array(
      StructField("library_name", StringType, true),
      StructField("location", StringType, true),
      StructField("books", ArrayType(StructType(Array(
        StructField("book_id", StringType, true),
        StructField("book_name", StringType, true),
        StructField("author", StringType, true),
        StructField("copies_available", IntegerType, true)
      )))),
      StructField("members", ArrayType(StructType(Array(
        StructField("member_id", StringType),
        StructField("member_name", StringType),
        StructField("age", IntegerType),
        StructField("books_borrowed", ArrayType(StringType)),
      ))))
    ))

    val df = spark.read.schema(schema).json(library_data)
    df.show(5, truncate = false)
    df.printSchema()
  }
}
