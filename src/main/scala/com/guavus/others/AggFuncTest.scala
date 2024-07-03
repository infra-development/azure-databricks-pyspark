package com.guavus.others

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

import java.util

object AggFuncTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Agg Function Test")
      .master("local[*]")
      .getOrCreate()

    val list = List(Row("2020-06-04"), Row("2020-07-05"), Row("2020-08-03"))
    val schema = new StructType(Array(StructField("csp_bus_date", StringType, true)))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(list), schema)
    df.show()

    val bus_dt = "2020-08-03"
    val output: Row = df.filter(col("csp_bus_date") < bus_dt).agg(max("csp_bus_date")).collectAsList().get(0)
    println(output)
  }

}
