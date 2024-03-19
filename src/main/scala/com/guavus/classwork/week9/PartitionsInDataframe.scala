package com.guavus.classwork.week9

import com.guavus.utility.AppConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object PartitionsInDataframe {
  def main(args: Array[String]): Unit = {
    // spark.sql.shuffle.partitions 200
    val spark = SparkSession
      .builder()
      .appName("Accessing Columns")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val schemaString = "order_id long, order_date string, cust_id long, order_status string"
    val schema = StructType.fromDDL(schemaString)
    val orders_path = AppConfig.orders_1gb

    val ordersDF = spark.read.schema(schema).csv(orders_path)
    ordersDF.show()
    println(ordersDF.rdd.getNumPartitions)

  }
}

/*
file_size/number_of_cores
partitions size should be min(128mb, 1.1gb/2)
partitions size should be min(128mb, file_size/number_of_cores(which is defaultParallelism))
spark.sql.files.maxPartitionBytes = 128mb
 */