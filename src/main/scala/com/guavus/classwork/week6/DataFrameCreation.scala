package com.guavus.classwork.week6

import com.guavus.utility.AppConfig
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}

object DataFrameCreation {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Dataframe creation")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val orders_sample_path = AppConfig.ordersSamplePath
    val orders_sample2_path = AppConfig.ordersSample2Path
    val customer_nested_path = AppConfig.customerNestedPath
    val order_item = AppConfig.orderItemPath

    val ordersSchemaString = "order_id LONG, order_date STRING, cust_id LONG, order_status STRING"
    val schema = StructType.fromDDL(ordersSchemaString)
    val orders_sample_df = spark.read.schema(ordersSchemaString).csv(orders_sample_path)
    val orders_sample_df1 = spark.read.schema(schema).csv(orders_sample_path)
    orders_sample_df.createOrReplaceTempView("orders_view")

    /*
    spark.read
    spark.sql
    spark.table
    spark.range
    spark.createDataFrame --> create dataframe from list
    spark.sparkContext.parallelize(list) --> creating rdd
     */

    // Apart from these ways we have seen so far few more ways to create dataframe
    val another_df = spark.table("orders_view")
    another_df.show()

    spark.range(5).show() // --> Single column dataframe --> column name = id
    spark.range(0, 8, 2).show() // --> Single column dataframe --> column name = id

    val list : List[(Long, String, Long, String)] = List(
      (1, "2013-07-25 00:00:00.0", 11599, "CLOSED"),
      (2, "2013-07-26 00:00:00.0", 11699, "PENDING_PAYMENT"),
      (3, "2013-07-27 00:00:00.0", 11799, "COMPLETE"),
    )

    val listDf = spark.createDataFrame(list).toDF("order_id", "order_date", "cust_id", "order_status")
    listDf.show()

    val ordersSchema1 = StructType(
      Array(
        StructField("order_id", LongType, nullable = false),
        StructField("order_date", StringType, nullable = true),
        StructField("cust_id", LongType, nullable = false),
        StructField("order_status", StringType, nullable = true)
      )
    )

    val ordersSchema2 = Array("order_id", "order_date", "cust_id", "order_status")

    val rows = list.map {
      case (order_id, order_date, cust_id, order_status) => Row(order_id, order_date, cust_id, order_status)
    }
    val rdd = spark.sparkContext.parallelize(rows)
    val listDf1 = spark.createDataFrame(rdd, ordersSchema1) // Best way for passing schema
    listDf1.show()

    // create dataframe from rdd (from reading file)
    val orders_sample_rdd = spark.sparkContext.textFile(orders_sample_path)
    val converted_rdd = orders_sample_rdd.map{x => {
      val splitRecord = x.split(",")
      Row(splitRecord(0).toLong, splitRecord(1), splitRecord(2).toLong, splitRecord(3))
    }}

    val listDF3 = spark.createDataFrame(converted_rdd, ordersSchema1)
    listDF3.show()

    // Nested Schema
    val ddlSchemaString = "customer_id long, fullname struct<firstname:string, lastname:string>, city string"
    val ddlSchema = StructType.fromDDL(ddlSchemaString)

    // Display the schema
    ddlSchema.printTreeString()

    val df_nested = spark.read.format("json").schema(ddlSchema).load(customer_nested_path)
    df_nested.show()

    val customer_schema = StructType(Array(
      StructField("customer_id", LongType, true),
      StructField("fullname", StructType(Array(
        StructField("firstname", StringType, true),
        StructField("lastname", StringType, true)
      ))),
      StructField("city", StringType, true)
    ))

    val df_nested2 = spark.read.format("json").schema(customer_schema).load(customer_nested_path)
    df_nested2.show()

    /*
    Add new column --> withColumn
    Rename --> withColumnRenamed
    Drop column --> drop
     */

    val df_row = spark.read.csv(order_item)
    df_row.show()
    df_row.printSchema()

    val df_final = df_row.toDF("order_item_id", "order_id", "product_id", "quantity", "subtotal", "product_price")
    df_final.show()

    val df1 = df_final.drop("subtotal")
    df1.show()

    //df1.select(col("*"), col("product_price * quantity as subtotal")).show() // Will not work because this an expression
    df1.select(col("*"), expr("product_price * quantity as subtotal")).show()
    df1.selectExpr("*", "product_price * quantity as subtotal").show()

    val products =AppConfig.products_path
    val schemaProduct = StructType(Array(
      StructField("product_id", IntegerType, true),
      StructField("product_category", IntegerType, true),
      StructField("product_name", StringType, true),
      StructField("product_description", StringType, true),
      StructField("product_price", DoubleType, true),
      StructField("product_image", StringType, true)
    ))

    val products_df = spark.read.schema(schemaProduct).csv(products)
    products_df.show()

    val new_product_df = products_df.withColumn("product_price", expr("CASE WHEN product_name like '%Nike%' THEN product_price * 1.2 WHEN product_name like '%Armour%' THEN product_price * 1.1 ELSE product_price END"))
    new_product_df.show()

    // Remove duplicate record from dataframe
    val mylist = Array((1, "Kapil", 34),
      (1, "Kapil", 34),
      (1, "Satish", 26),
      (2, "Satish", 26))

    val myListDf: DataFrame = spark.createDataFrame(mylist).toDF("id", "name", "age")
    myListDf.show()

    val df1_new = myListDf.distinct
    df1_new.show()

    val df2_new = myListDf.dropDuplicates(Array("name", "age"))
    df2_new.show()

    val df3_new = myListDf.dropDuplicates("id")
    df3_new.show()

    /*
    Spark Session
    It's entry point to spark cluster
    Used for High level API Dataframe, Spark SQL
    Instead of having sparkContext, hiveContext, sqlContext --> Everything is now encapsulated in single spark session
     */

    // We need spark context when dealing at RDD level
    val spark2 = spark.newSession() // --> for another session

    // Both spark session have different name spaces
  }

}
