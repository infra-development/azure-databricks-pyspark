package com.guavus.assignments.week5

import com.guavus.utility.AppConfig
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, desc, length}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

import scala.Console.println

object W5Question3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Week5 Question3")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    val schema = StructType(
      Array(StructField("cust_id", IntegerType, true),
        StructField("cust_fname", StringType, true),
        StructField("cust_lname", StringType, true),
        StructField("cust_email", StringType, true),
        StructField("cust_password", StringType, true),
        StructField("cust_street", StringType, true),
        StructField("cust_city", StringType, true),
        StructField("cust_state", StringType, true),
        StructField("cust_zipcode", IntegerType, true),
      )
    )

    val customer_df = spark.read.option("header", "true").schema(schema).csv(AppConfig.customersPath)
    customer_df.show()
    customer_df.createOrReplaceTempView("customer_temp")

    // 3.1 Find the total number of customers in each state
    customer_df.groupBy("cust_state").count().orderBy(desc("count")).show()
    spark.sql("select cust_state, count(*) cnt from customer_temp group by cust_state order by cnt desc").show()

    // 3.2 FInd the top 5 most common last names among the customers
    customer_df.groupBy("cust_lname").count().orderBy(desc("count")).show()
    spark.sql("select cust_lname, count(*) cnt from customer_temp group by cust_lname order by cnt desc").show()

    // 3.3. Check whether there are any customers whose zip codes are not valid(i.e not equal to 5 digits)
    customer_df.filter(length(col("cust_zipcode")) =!= 5).show()
    spark.sql("select * from customer_temp where length(cust_zipcode) != 5").show()

    //  3.4 Count the number of customers who have valid zip codes.
    println("number of customer with zipcode : --> " + customer_df.filter(length(col("cust_zipcode")) === 5).count())
    spark.sql("select count(*) from customer_temp where length(cust_zipcode) == 5").show()

    // 3.5. Find the number of customers from each city in the state of California(CA)
    customer_df.filter("cust_state = 'CA'").groupBy("cust_city").count().show()
    spark.sql("select cust_city, count(*) from customer_temp where cust_state = 'CA' group by cust_city").show()

  }
}
