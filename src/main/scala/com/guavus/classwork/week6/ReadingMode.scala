package com.guavus.classwork.week6

import com.guavus.utility.AppConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object ReadingMode {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Reading Modes").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val orders_sample_path = AppConfig.ordersSamplePath
    val orders_sample2_path = AppConfig.ordersSample2Path
    val orders_sample3_path = AppConfig.ordersSample3Path

    /*
    Reading Mode
    1. Permissive, 2. dropmalformed, 3. Failfast (PDF)
    Allows Incorrect records with null value (default)
    Drops incorrect records
    Fails with exception for incorrect record
     */

    val order_schema = StructType(Array(
      StructField("order_id", LongType, true),
      StructField("order_date", StringType, true),
      StructField("customer_id", LongType, true),
      StructField("order_status", StringType, true)
    ))

    // 1. Permissive
    val order_sample_df = spark.read
      .schema(order_schema)
      .option("mode", "permissive")
      .csv(orders_sample3_path)
    order_sample_df.show()

    // 2. dropmalformed
    val orders_sample_df2 = spark.read
      .schema(order_schema)
      .option("mode", "dropmalformed")
      .csv(orders_sample3_path)
    orders_sample_df2.show()

    // 3. failfast
//    val orders_sample_df3 = spark.read.schema(order_schema).option("mode", "failfast").csv(orders_sample3_path)
//    orders_sample_df3.show()
  }
}
