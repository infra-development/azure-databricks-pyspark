package com.guavus.classwork.week9

import com.guavus.utility.AppConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object WriterAPI {
  def main(args: Array[String]): Unit = {
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

    /*
    4 modes while writing
    1. overwrite, 2. ignore, 3. append, 4. errorIfExists
    default write to parquet

    Do partition by on column which has less distinct values
    Do bucketing on column which has more distinct values
     */
    spark.sql("create database if not exits haresh_demo")
    spark.sql("use haresh_demo")
    println(ordersDF.rdd.getNumPartitions)
    ordersDF.write.mode("overwrite").csv("D:/repos/azure-spark-scala/spark-warehouse/orders")
    ordersDF.write.partitionBy("order_status").bucketBy(4, "order_id").mode("overwrite").saveAsTable("haresh_demo.orders1")
    ordersDF.write.partitionBy("order_status").mode("overwrite").format("parquet").save("D:/repos/azure-spark-scala/spark-warehouse/orders2")

  }
}
