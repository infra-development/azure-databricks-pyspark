package com.guavus.classwork.week4

import com.guavus.utility.AppConfig
import org.apache.spark.sql.SparkSession

object SparkObjectRDD {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("RDD")
      .master("local[*]")
      .getOrCreate

    spark.sparkContext.setLogLevel("WARN")

    val ordersPath = AppConfig.ordersPath
    val order_rdd = spark.sparkContext.textFile(ordersPath)

    /*
    # Schema
    ################################################################
    # order_id, date, customer_id, order_status
    */

    //1. Orders in each category
    println("1. Orders in each category")
    val order_category_rdd = order_rdd.map(order => (order.split(",")(3), 1))

    val reduced_category_rdd = order_category_rdd.reduceByKey((a, b) => a + b)

    val sorted_order_rdd = reduced_category_rdd.sortBy((order => order._2), false)

    sorted_order_rdd.collect().foreach(x => println(x._1 + " " + x._2))

    //2. Find Premium Customers
    println("2. Find Premium Customers")
    val premium_rdd = order_rdd.map(order => (order.split(",")(2), 1))

    val reduced_premium_rdd = premium_rdd.reduceByKey((a, b) => (a + b))

    val sorted_premium_rdd = reduced_premium_rdd.sortBy((order => order._2), false)

    sorted_premium_rdd.take(10).foreach(x => println(x._1 + " " + x._2))

    // 3. Distinct Customer who placed at least 1 order
    println("3. Distinct Customer who placed at least 1 order")
    val customer_rdd = order_rdd.map(order => order.split(",")(2))

    val distinct_customer_rdd = customer_rdd.distinct()

    distinct_customer_rdd.take(10).foreach(x => println(x))

    // 4. Customers having maximum number of closed orders
    println("4. Customers having maximum number of closed orders")
    val customers_with_closed_rdd = order_rdd.filter(order => (order.split(",")(3) == "CLOSED"))

    val mapped_customers = customers_with_closed_rdd.map(order => (order.split(",")(2), 1))

    val reduced_customers = mapped_customers.reduceByKey((a, b) => a + b)

    val sorted_customers = reduced_customers.sortBy((order => order._2), false)

    sorted_customers.take(10).foreach(x => println(x._1 + " " + x._2))
  }
}
