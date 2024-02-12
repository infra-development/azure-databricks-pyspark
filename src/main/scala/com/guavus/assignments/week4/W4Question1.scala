package com.guavus.assignments.week4

import com.guavus.utility.AppConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object W4Question1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Week4 Question1")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val orders_rdd = spark.sparkContext.textFile(AppConfig.ordersPath)
    val customers_rdd = spark.sparkContext.textFile(AppConfig.customersPath)
    val order_item_rdd = spark.sparkContext.textFile(AppConfig.orderItemPath)

    /*
    orders
    order_id || order_date || customer_id || order_status

    customers
    customer_id || customer_fname || customer_lname || customer_email || customer_password || customer_street ||customer_city ||customer_state || customer_zipcode

    order_items
    order_item_id || order_id || order_item_product_id || order_item_quantity || order_item_subtotal || order_item_product_price
     */

    //  1. we need to find top 10 customers who have spent the most amount (premium customers)
    val order_id_subtotal_rdd = order_item_rdd.map(x => (x.split(",")(1), x.split(",")(4)))
    val order_id_customer_id_rdd = orders_rdd.map(x => (x.split(",")(0), x.split(",")(2)))

    val order_id_subtotal_customer_id_rdd = order_id_subtotal_rdd.join(order_id_customer_id_rdd)
    order_id_subtotal_customer_id_rdd.take(10).foreach(x => println(x))
    val customer_id_subtotal_rdd = order_id_subtotal_customer_id_rdd.map(x => (x._2._2, x._2._1.toDouble))
    customer_id_subtotal_rdd.take(5).foreach(x => println(x))

    val reduced_customer_subtotal = customer_id_subtotal_rdd.reduceByKey((x, y) => x + y).sortBy(x => x._2, ascending = false)
    reduced_customer_subtotal.take(5).foreach(x => println(x))

    /*
    Another way
    reduced_customer_subtotal.take(5).foreach{
      case (key, value) => println(s"($key, ${value.formatted("%.2f")})")
    }
     */

    // 2. Top 10 product id's with most quantities sold
    val product_quantity_rdd = order_item_rdd.map(x => (x.split(",")(2), x.split(",")(3).toDouble))
    product_quantity_rdd.reduceByKey((x, y) => x + y).sortBy(x => x._2).take(10).foreach(println)

    // 3. How many customers are from Caguas city
    val customer_caguas_rdd = customers_rdd.filter(x => x.split(",")(6) == "Caguas").count()
    println(customer_caguas_rdd)

    // 4. Top 3 states with maximum customers
    val state_customer_rdd = customers_rdd.map(x => (x.split(",")(7), x.split(",")(0).toInt))
    state_customer_rdd.reduceByKey((x, y) => x + y).sortBy(x => x._2, ascending = false).take(3).foreach(println)

    // 5. How many customers have spent more than $1000 in total
    val order_subtotal_rdd = order_item_rdd.map(x => (x.split(",")(1), x.split(",")(4).toDouble))
    val order_customer_rdd = orders_rdd.map(x => (x.split(",")(0), x.split(",")(2)))

    val order_customer_subtotal_rdd = order_customer_rdd.join(order_subtotal_rdd)
    val customer_subtotal_rdd = order_customer_subtotal_rdd.map(x => (x._2._1, x._2._2))
    println(customer_subtotal_rdd.reduceByKey((x, y) => x + y).filter(x => x._2 > 1000).count())

    // 6. Which state has most number of orders in CLOSED status
    val customer_closed_rdd = orders_rdd.map(x => (x.split(",")(2), x.split(",")(3))).filter(x => x._2 == "CLOSED")
    val customer_state_rdd = customers_rdd.map(x => (x.split(",")(0), x.split(",")(7)))

    println("----------(customer_id, (status, state))------------")
    val customer_status_state_rdd = customer_closed_rdd.join(customer_state_rdd)
    val state_status_rdd = customer_status_state_rdd.map(x => (x._2._2, 1))
    state_status_rdd.reduceByKey((x, y) => x + y).sortBy(x => x._2, ascending = false).take(10).foreach(println)

    // 7. How many customers are active (active customers are the one's who placed at least one order)
    // Following is straight looking answer but less performant
    println(orders_rdd.map(x => x.split(",")(2)).distinct().count()) //--> Distinct is wide transformation
    println(orders_rdd.map(x => (x.split(",")(2), 1)).reduceByKey((x, y) => x + y).count()) // --> reduceByKey is narrow transformation

    // 8. What is the revenue generated in each state in sorted order
    val order_subtotal = order_item_rdd.map(x => (x.split(",")(1), x.split(",")(4).toDouble))
    val order_customer = orders_rdd.map(x => (x.split(",")(0), x.split(",")(2)))
    val customer_state = customers_rdd.map(x => (x.split(",")(0), x.split(",")(7)))

    val order_customer_subtotal = order_customer.join(order_subtotal)
    val customer_subtotal = order_customer_subtotal.map(x => (x._2._1, x._2._2))

    val customer_state_subtotal = customer_state.join(customer_subtotal)
    val state_subtotal = customer_state_subtotal.map(x => (x._2._1, x._2._2))

    state_subtotal.reduceByKey((x, y) => x +y).sortBy(x => x._2, ascending = false).take(10).foreach{
      case (key, value) => println(s"$key, ${value.formatted("%.2f")}")
    }
  }
}
