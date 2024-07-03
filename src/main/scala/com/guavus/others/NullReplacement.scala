package com.guavus.others

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, current_date, current_timestamp, isnull, to_date, to_timestamp, when}

object NullReplacement {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Accessing Columns")
      .master("local[*]")
      .getOrCreate()

    val list : List[(Long, String, Long, String)] = List(
      (1, "2013-07-25 00:00:00.0", 11599, "CLOSED"),
      (2, null, 11699, "PENDING_PAYMENT"),
      (3, "2013-07-27 00:00:00.0", 11799, "COMPLETE"),
    )

    val df = spark.createDataFrame(list).toDF("id", "date","cust_id", "status")
//    val df1 = df.withColumn("date", to_timestamp(col("date")))
//    df1.show()
//    df1.printSchema()
//
//    val newDF = df1.withColumn("date", when(col("date").isNull, current_timestamp()).otherwise(col("date")))
//    newDF.show(false)
//
//    val newDF2 = df1.withColumn("date", customizedDate(col("date")))
//    newDF2.show(false)

    val columnMap = Map("id" -> "user_id", "date" -> "record_date", "cust_id" -> "customer_id", "status" -> "order_status")
    val map1 = Map("id" -> 1, "date" -> 2, "cust_id" -> 3, "status" -> 4)

    val newDF = renameColumns(df, columnMap)
    newDF.show()




  }

  private def renameColumns(df: DataFrame, columnMap: Map[String, String]): DataFrame = {
    columnMap.foldLeft(df) { case (tempDF, (oldName, newName)) =>
      tempDF.withColumnRenamed(oldName, newName)
    }
  }

  private def customizedDate(column: Column) : Column = {
    when(column.isNull, to_date(current_timestamp())).otherwise(to_date(column))
  }
}
