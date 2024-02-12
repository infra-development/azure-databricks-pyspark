package com.guavus.assignments.week5

import com.guavus.utility.AppConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object W5Question2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Week5 Question2")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    val schema = StructType(
      Array(StructField("ProductId", IntegerType, true),
        StructField("category", StringType, true),
        StructField("ProductName", StringType, true),
        StructField("Description", StringType, true),
        StructField("Price", DoubleType, true),
        StructField("ImageURL", StringType, true)
      )
    )

    val products_df = spark.read.option("header", "true").schema(schema).csv(AppConfig.products_path)
    products_df.printSchema()
    products_df.createOrReplaceTempView("product_temp")
    products_df.show()

    // 2.1 Find total number of products in the given datasets
    println(products_df.count())
    spark.sql("select count(*) from product_temp").show(truncate = false)

    // 2.2 Find the number of unique categories of products in the given dataset
    println(products_df.select("category").distinct().count())
    spark.sql("select count(distinct category) from product_temp").show(truncate = false)

    // 2.3 Find the top 5 most expensive products based on their price, along with their product name, category, and image URL.
    products_df.select("ProductName", "Category", "imageURL", "Price")
      .orderBy(desc("price"))
      .limit(5).show()
    spark.sql("select productname, category, imageurl, price from product_temp order by price desc limit 5").show()

    // 2.4 Find the number of products in each category that have a price greater than $100. Display the results in a tabular format that shows the category name and the number of products that satisfy the condition
    products_df.filter("price > 100").groupBy("category").count().withColumnRenamed("count", "no_of_columns").orderBy(desc("no_of_columns"))show()
    spark.sql("select category, count(*) as no_of_columns from product_temp where price > 100 group by category order by no_of_columns desc").show()

    //2.5. What are the product names and prices of products that have a price greater than $200 and belong to category 5?
    products_df.select("productname", "price").where("price > 200 and category = 5").show()
    spark.sql("select productname, price from product_temp where price > 200 and category = 5").show()

  }

}
