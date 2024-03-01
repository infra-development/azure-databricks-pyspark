package com.guavus.classwork.week8

import com.guavus.utility.AppConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, count, countDistinct, expr, sum}
import org.apache.spark.sql.types.{IntegerType, StructType}

object AggregateFunctions {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Accessing Columns")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val orders_path = AppConfig.order_data

    val order_df = spark.read.option("inferSchema", "true").option("header", "true").csv(orders_path)

    order_df.show()
    order_df.printSchema()
    order_df.withColumn("invoiceno", col("invoiceno").cast(IntegerType)).printSchema()
    val order_new_df = order_df.withColumn("invoiceno", col("invoiceno").cast(IntegerType))
    order_new_df.printSchema()
    order_new_df.show()

    /*
    1. Simple aggregations --> will give only one output row, like count total records, sum of quantity
    2. Grouping Aggregation --> will do group by
    3. Windowing functions -->
     */

    println(order_new_df.count())
    order_new_df.select(count("*").alias("row_count"),
      countDistinct("invoiceno").alias("unique_invoice"),
      sum("quantity").alias("total_quantity"),
      avg("unitprice").alias("avg_price")).show()

    order_new_df.selectExpr("count(*) as row_count",
      "count(distinct invoiceno) as unique_invoice",
      "sum(quantity) as total_quantity",
      "avg(unitprice) as avg_price").show()

    order_new_df.createOrReplaceTempView("orders_view")
    spark.sql("select count(*) as row_count, " +
      "count(distinct invoiceno) as unqiue_invoice, " +
      "sum(quantity) as total_quantity, " +
      "avg(unitprice) as avg_price from orders_view").show()

    println("------------------------------------------------")

    order_new_df.show()

    order_new_df.groupBy("country", "invoiceno")
      .agg(sum("quantity").alias("total_quantity"), sum(expr("quantity * unitprice")).alias("invoice_value"))
      .sort("invoiceno").show(40)


    order_new_df.groupBy("country", "invoiceno")
      .agg(expr("sum(quantity) as total_quantity"), sum(expr("quantity * unitprice as invoice_value")))
      .sort("invoiceno").show(40)

    order_new_df.select("*").where("invoiceno is null").show()

  }
}
