package com.guavus.assignments.week6

import com.guavus.utility.AppConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object W6Question4 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Week6 Question4")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    val sales_path = AppConfig.sales_data

    val schemaString = "store_id integer, product string, quantity integer, revenue double"
    val schema = StructType.fromDDL(schemaString)
    val df = spark.read.schema(schema).json(sales_path) // default mode is permissive
    df.show(truncate = false)

    val df1 = spark.read.option("mode", "dropmalformed").schema(schema).json(sales_path)
    df1.show(truncate = false)
    println("Data count : "+df1.count())

  }

}
