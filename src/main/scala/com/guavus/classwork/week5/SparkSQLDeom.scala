package com.guavus.classwork.week5

import com.guavus.utility.AppConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object SparkSQLDeom {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL Demo")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val orders_file_path = AppConfig.ordersPath
    val orders_df = spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(orders_file_path)

    orders_df.createOrReplaceTempView("orders_view")

    spark.sql("create database if not exists haresh_demo")
    spark.sql("show databases").show()
    spark.sql("show databases").filter("namespace = 'haresh_demo'").show()
    spark.sql("show databases").filter("namespace like 'haresh%'").show()

    spark.sql("show tables").show()
    //spark.sql("use haresh_demo")
    spark.sql("show tables").show()

    spark.sql("create table if not exists haresh_demo.orders (order_id integer, order_date string, customer_id integer, order_status string)")
    spark.sql("show tables").show()

    // Inserting data into order table from the view created orders_view
    //spark.sql("insert into haresh_demo.orders select * from orders_view") --> insert by selecting another table
    spark.sql("select * from haresh_demo.orders limit 5").show()
    spark.sql("show tables").show()

    // Managed table
    // Data stored at file location spark-warehouse/haresh_demo.db/order
    spark.sql("describe extended haresh_demo.orders").show(truncate= false)

    spark.sql("drop table haresh_demo.orders")
    // spark.sql("describe extended haresh_demo.orders").show(truncate=False) --> Exception (table not found)
    spark.sql("show tables").show() // table is gone, files not present at warehouse location

    // Managed vs External Table
    //spark.sql("create table haresh_demo.orders (order_id integer, order_date, string, customer_id integer,orders_status string)")

    //spark.sql("create table if not exists haresh_demo.new_orders (order_id integer, order_date string, customer_id integer, orders_status string) using csv location 'D:/files/orders_wh/orders_wh.csv'")
//    spark.sql("select * from haresh_demo.new_orders limit 5").show()
//    spark.sql("describe extended haresh_demo.new_orders").show(truncate =false)

    // In case of external table when you drop table --> you only drop metadata, data is not dropped.
    // In case of managed table when you drop table --> you drop metadata + data

    // spark.sql("truncate table haresh_demo.new_orders") --> operation not allowed exception

    // DML Operation
    // Insert --> Working fine --> New file created at location orders/
    //spark.sql("insert into table haresh_demo.new_orders values (1111, '12-02-2023', 2222, 'CLOSED')")

    // In case of open source spark
    // Update / Delete --> Don't work

    // In case of databricks update and delete works


  }

}
