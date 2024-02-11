package com.guavus.classwork.week5

import com.guavus.utility.AppConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object HigherAPIQuestions {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Higher API")
      .master("local[*]")
      .getOrCreate()

    val order_file = AppConfig.ordersPath
    spark.sparkContext.setLogLevel("WARN")

    val orders_df = spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(order_file)

    orders_df.createOrReplaceTempView("orders_view")

    // 1. Top 15 customers who placed the most number of orders
    orders_df.groupBy("customer_id").count().sort(desc("count")).limit(15).show()
    spark.sql("select customer_id, count(*) cnt from orders_view group by customer_id order by cnt desc limit 15").show()

    // 2. Find the number of orders under each order status
    orders_df.groupBy("order_status").count().show()
    spark.sql("select order_status, count(*) from orders_view group by order_status").show()

    // 3. Find the number of active customers
    println(orders_df.select("customer_id").distinct().count())
    spark.sql("select count(distinct customer_id) from orders_view").show()

    // Count with group by is transformation otherwise it's an action

    // 4. Customer with most number of closed orders
    orders_df
      .filter("order_status='CLOSED'")
      .groupBy("customer_id").count()
      .sort(desc("count"))
      .withColumnRenamed("count", "cnt").show()

    spark.sql("select customer_id, count(*) from orders_view where order_status='CLOSED' group by customer_id").show()

    /*
    order by --> Transformation
    filter --> Transformation
    show --> action
    head --> action
    tail --> action
    take --> action
    collect --> action
    distinct --> transformation
    join --> transformation
    printSchema --> Utility function (Neither transformation nor actions)
    cache --> Utility function
    createOrReplaceTempView --> Utility function
     */
  }
}
