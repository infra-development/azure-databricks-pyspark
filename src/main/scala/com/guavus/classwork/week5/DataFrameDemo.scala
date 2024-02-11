package com.guavus.classwork.week5

import com.guavus.utility.AppConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column


object DataFrameDemo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Data Frame Demo")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val orderFilePath = AppConfig.ordersPath
    val ordersDf = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(orderFilePath)

    /*
    inferSchema is not advised as it might not infer correctly sometimes,
    and it takes time to infer as spark has to scan the data in order to determine the data type
    */

    val ordersDfDemo = spark.read.csv(orderFilePath)
    /*
    Same way
    val ordersDf_demo2 = spark.read.json(orderFilePath) --> Column name and data are embedded
    val ordersDf_demo3 = spark.read.parquet(orderFilePath) --> Column name and data are embedded, column based file format works very well with spark
    val ordersDf_demo4 = spark.read.orc(orderFilePath) --> Column name and data are embedded
    spark.read.jdbc
    spark.read.table
     */

    ordersDf.show(1)
    ordersDf.printSchema()

    // Rename the order_status to status
    val renamed_status = ordersDf.withColumnRenamed("order_status", "status")
    renamed_status.show(1)

    // Create new column from existing column and applying function on it
    val newColumn = renamed_status.withColumn("order_date_new", to_timestamp(col("order_date")))
    newColumn.show(1)

    val filtered_df = ordersDf.where("customer_id = 11599")
    filtered_df.show(truncate = false)

    // Another way
    val filteredDf = ordersDf.filter("customer_id = 11599")
    filteredDf.show(truncate = false)

    // Create dataframe into sql table or view
    ordersDf.createOrReplaceTempView("orders_view")
    val filtered_sql_df = spark.sql("select * from orders_view where order_status = 'CLOSED'")
    filtered_sql_df.show(truncate = false)

    // Convert sql table to dataframe
    val orders_dataframe = spark.read.table("orders_view")
    orders_dataframe.show()
  }
}
