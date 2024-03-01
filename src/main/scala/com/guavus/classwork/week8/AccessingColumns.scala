package com.guavus.classwork.week8

import com.guavus.utility.AppConfig
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{col, column, expr}
import org.apache.spark.sql.types.StructType

object AccessingColumns {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Accessing Columns")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    val orders_path = AppConfig.ordersPath
    val schemaString = "order_id long, order_date string, cust_id long, order_status string"
    val schema = StructType.fromDDL(schemaString)
    val orders_df = spark.read.schema(schema).option("header", "true").csv(orders_path)

    orders_df.show()
    orders_df.select("*").show()

    orders_df.select("order_id", "order_date").show()
    orders_df.select(orders_df("order_id"), orders_df("order_date")).show()
    orders_df.select(orders_df.col("order_id"), orders_df.col("order_date")).show()
    orders_df.select(expr("order_id"), expr("order_status")).show()
    orders_df.select(expr("order_id"), expr("cust_id + 1 as new_id"), expr("cust_id")).show()
    orders_df.select(col("order_id"), col("order_status")).show()
    orders_df.select(column("order_id"), column("order_status")).show()

    val selectedColumns: Seq[Column] = Seq("order_id", "order_date").map(c => col(c))
    orders_df.select(selectedColumns: _*).show()

    // select the first or last 2 columns
    orders_df.selectExpr(orders_df.columns.take(2): _*).show()
    orders_df.selectExpr(orders_df.columns.takeRight(2): _*).show()
  }
}
