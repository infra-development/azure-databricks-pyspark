package com.guavus.others

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{col, concat, desc, floor, lit, rand, when}

import scala.util.Random

object SkewedData {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Accessing Columns")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val skewedData = List.fill(1000)("A") ::: List.fill(100)("B") ::: List.fill(10)("C") ::: List("D")

    import spark.implicits._
    val df = skewedData.toDF("Category")
    val skewedField: Any = df.groupBy("Category").count().orderBy(desc("count")).first()(0)
    println(skewedField)

    val saltedData = df
      .withColumn("Category",
        when(col("Category") === skewedField, concat(col("Category"), floor(rand()*10)))
          .otherwise(col("Category")))

    saltedData.groupBy("Category").count().orderBy(desc("count")).show()
  }

}
