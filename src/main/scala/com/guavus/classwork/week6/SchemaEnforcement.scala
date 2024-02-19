package com.guavus.classwork.week6

import com.guavus.utility.AppConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}

object SchemaEnforcement {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Schema Enforcement")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val orderSamplePath = AppConfig.ordersSamplePath
    val orderSample2Path = AppConfig.ordersSample2Path

    // samplingRation 0.1 scans 10% of total data to inferSchema
    // samplingRation 1 scans 100% of total data to inferSchema

    val orders_df = spark.read
      .option("inferSchema", "true")
      .option("samplingRation", 0.1)
      .option("header", "true")
      .csv(orderSamplePath)

    val orders_schema = StructType(Array(
      StructField("order_id", IntegerType, true),
      StructField("order_date", DateType, true),
      StructField("customer_id", IntegerType, true),
      StructField("order_status", StringType, true)
    ))

    val orders_sample_df = spark.read.schema(orders_schema).csv(orderSamplePath)
    orders_sample_df.show()
    orders_sample_df.printSchema()

    val orders_sample_df2 = spark.read.schema(orders_schema).csv(orderSamplePath)
    orders_sample_df2.show()
    orders_sample_df2.printSchema()

    // Give dateFormat if data contains another date format
    val orders_sample_df3 = spark.read.schema(orders_schema).option("dateFormat", "mm-dd-yyyy").csv(orderSample2Path)
    orders_sample_df3.show()
    orders_sample_df3.printSchema()

    // Load date as string and then apply str --> date
    val orders_sample3_df = spark.read.schema(orders_schema).csv(orderSample2Path)
    orders_sample3_df.printSchema()

    // create new column
    val new_df = orders_sample3_df.withColumn("order_date_new", to_date(col("order_date"), "mm-dd-yyyy"))
    new_df.printSchema()

    // Change existing column to date
    val new_df2 = orders_sample3_df.withColumn("order_date", to_date(col("order_date"), "mm-dd-yyyy"))
    new_df2.printSchema()





  }

}
