package com.guavus.assignments.week5

import com.guavus.utility.AppConfig
import org.apache.spark.sql.SparkSession

object W5Question1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Week5 Question1")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    val groceries_df = spark.read
      .option("inferSchema","true")
      .option("header", "true")
      .csv(AppConfig.groceries_path)
    groceries_df.createOrReplaceTempView("groceries")

    spark.sql("create database if not exists haresh_demo")
    spark.sql("show databases").show()
    spark.sql("use haresh_demo")
    spark.sql("create table if not exists haresh_demo.groceries_new (order_id string, location string, item string, order_date string, quantity integer)")
    spark.sql("insert into table haresh_demo.groceries_new select * from groceries")
    spark.sql("select count(*) from groceries_new").show()

    spark.sql("create table if not exists haresh_demo.groceries_ext (order_id string, location string, item string, order_date string, quantity integer) using csv options(header='true') location '/home/haresh/work/files/groceries/groceries.csv'")
    spark.sql("select * from haresh_demo.groceries_ext limit 10").show()
    spark.sql("select * from haresh_demo.groceries_new limit 10").show()
    spark.sql("describe extended haresh_demo.groceries_ext").show(truncate=false)

    spark.sql("drop table haresh_demo.groceries_ext")
    spark.sql("drop table haresh_demo.groceries_new")

    spark.sql("show tables").show()



  }

}
