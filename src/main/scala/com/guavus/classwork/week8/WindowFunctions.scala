package com.guavus.classwork.week8

import com.guavus.utility.AppConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object WindowFunctions {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Accessing Columns")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val window_path = AppConfig.window_data
    val window_modified_path = AppConfig.window_data_modfied


    val window_df = spark.read.option("inferSchema", "true").option("header", "true").csv(window_path)
    val window_modified_df = spark.read.option("inferSchema", "true").option("header", "true").csv(window_modified_path)

    // Windowing aggregation
    window_df.sort("country").show()

    println("---------- running total ---------------")

    val myWindow = Window.partitionBy("country").orderBy("weeknum").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    // current row and previous 1
    val myWindow1 = Window.partitionBy("country").orderBy("weeknum").rowsBetween(-2, Window.currentRow)
    val myWindow2 = Window.partitionBy("country").orderBy("weeknum")
    val result_df = window_df.withColumn("running_total", format_number(sum("invoicevalue").over(myWindow), 2))
    result_df.show()

    println("---------- previous_value and diff_from_previous ---------------")

    val result_df1 = window_df
      .withColumn("previous_value", lag("invoicevalue", 1).over(myWindow2))
      .withColumn("diff_from_previous", format_number(col("invoicevalue") - col("previous_value"), 2))
    result_df1.show()
    window_df.printSchema()
    result_df1.printSchema()

    println("---------- running_total ---------------")

    window_modified_df.orderBy("country", "invoicevalue").show()
    val anotherWindow = Window.partitionBy("country").orderBy("weeknum").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    val result_df2 = window_modified_df.withColumn("running_total", format_number(sum("invoicevalue").over(anotherWindow), 2))
    result_df2.show()

    println("---------- rank by invoicevalue per country  ---------------")

    val rank_window = Window.partitionBy("country").orderBy(desc("invoicevalue"))
    val rank_result_df = window_modified_df.withColumn("rank", rank().over(rank_window))
    rank_result_df.show()

    println("---------- row_num by invoicevalue per country  ---------------")

    val row_num_window = Window.partitionBy("country").orderBy(desc("invoicevalue"))
    val row_num_result_df = window_modified_df.withColumn("row_num", row_number().over(row_num_window))
    row_num_result_df.show()

    println("---------- dense_rank by invoicevalue per country  ---------------")

    val dense_rank_window = Window.partitionBy("country").orderBy(desc("invoicevalue"))
    val dense_rank_result_df = window_modified_df.withColumn("dense_rank", dense_rank().over(dense_rank_window))
    dense_rank_result_df.show()

    dense_rank_result_df.select("*").where("dense_rank = 1").drop("dense_rank").show()

    println("---------- prev_week and invoice_diff  ---------------")
    val lag_lead_window = Window.partitionBy("country").orderBy("weeknum")
    val lag_window_df = window_modified_df.withColumn("previous_week", lag("invoicevalue", 1).over(lag_lead_window))
    val lag_final_df = lag_window_df.withColumn("invoice_diff", expr("invoicevalue - previous_week"))
    lag_final_df.show()

    lag_final_df.select("*").where("invoice_diff is not null").show()

    val lead_window_df = window_modified_df.withColumn("next_week", lead("invoicevalue", 1).over(lag_lead_window))
    val lead_final_df = lead_window_df.withColumn("invoice_diff", expr("invoicevalue - next_week"))
    lead_final_df.show()

    lead_final_df.select("*").where("invoice_diff is not null").show()
    window_df.groupBy("country").pivot("weeknum").sum("invoicevalue").show()
    window_df.groupBy("country").pivot("weeknum").sum("invoicevalue").na.drop().show()
  }

}
