package com.guavus.classwork.week11

import com.guavus.utility.AppConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object SortVsHashAggregation {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Accessing Columns")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val schemaString = "order_id long, order_date string, customer_id long, order_status string"
    val schema = StructType.fromDDL(schemaString)
    val orders_path = AppConfig.orders_1gb

    val ordersDF = spark.read.schema(schema).csv(orders_path)
    ordersDF.createOrReplaceTempView("orders")

    spark.sql("""select customer_id, date_format(order_date, 'MMMM') as order_month, count(1) as total_cnt, first(int(date_format(order_date, 'MM'))) as month_num from orders group by customer_id, order_month order by month_num""").show()

    //The query which took more than 1 min did sort aggregate, the query which took laser time did hash aggregate.

    // Why hash aggregate is used just by casting column to the int
    // In terms of sort aggregate
  }
}
