package com.guavus.classwork.week4

import com.guavus.utility.AppConfig
import org.apache.spark.sql.SparkSession

object PartitionsCheck {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("partitions-check")
      .master("local[*]")
      .getOrCreate()

    val ordersPath = AppConfig.ordersPath

    val order_rdd = spark.sparkContext.textFile(ordersPath)

    println("getNumberOfPartitions = " + order_rdd.getNumPartitions)

    println("defaultMinPartitions = " + spark.sparkContext.defaultMinPartitions)

    /*
    No.of default tasks that can run in parallel
    Mainly number of cores available
     */
    println("defaultParallelism = " + spark.sparkContext.defaultParallelism)

    /*
    countByValue --> map + reduceByKey
    countByValue --> Action


    map, filter, flatMap --> Narrow Transformation

    reduceByKey, groupByKey --> Wide Transformation

    Broadcast the dataset which lower in size, so it will be available on all the nodes for join and won't occupy more space

    spark.sparkContext.broadcast(customers.collect())
    joined_rdd = orders.join(customers)
     */


    //Repartition Vs Coalesce

    /*
    Repartitioning is the process of changing the number of partitions of an RDD either increasing or decreasing.
    Coalescing is the process of changing the number of partitions of an RDD only for decreasing.
    Repartitioning or Coalesce can be done by specifying the number of partitions.

    Coalesce is more performant compared to repartition because it tries to merge partitions on same node to form a new partition that could be of unequal size but shuffling is avoided.
    Repartition do complete shuffling with intent to have equal partition size.
     */

    val repartitioned_rdd = order_rdd.repartition(6)
    val coalesced_rdd = order_rdd.coalesce(1)
  }
}
