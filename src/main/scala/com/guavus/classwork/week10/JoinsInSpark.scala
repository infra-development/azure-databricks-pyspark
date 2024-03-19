package com.guavus.classwork.week10

import com.guavus.utility.AppConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object JoinsInSpark {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Accessing Columns")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val schemaString = "order_id long, order_date string, cust_id long, order_status string"
    val schema = StructType.fromDDL(schemaString)
    val orders_path = AppConfig.orders_1gb
    val customer_path = AppConfig.customersPath

    val schemaCustomer = StructType(
      Array(StructField("customerid", IntegerType, true),
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

    val ordersDF = spark.read.schema(schema).csv(orders_path)
    val customersDF = spark.read.schema(schemaCustomer).csv(customer_path)

    // broadcast join possible for inner and left join
    ordersDF.join(customersDF, ordersDF("cust_id") === customersDF("customerid"), "inner").write.format("noop").mode("overwrite").save()
    ordersDF.join(customersDF, ordersDF("cust_id") === customersDF("customerid"), "left").write.format("noop").mode("overwrite").save()

    // forcefully broadcast
    ordersDF.join(broadcast(customersDF), ordersDF("cust_id") === customersDF("customerid"), "left")
      .write.format("noop").mode("overwrite").save()

    // broadcast join is not possible for right and outer join only shuffle-short-merge join will happen
    ordersDF.join(customersDF, ordersDF("cust_id") === customersDF("customerid"), "right").write.format("noop").mode("overwrite").save()
    ordersDF.join(customersDF, ordersDF("cust_id") === customersDF("customerid"), "full").write.format("noop").mode("overwrite").save()

    // left semi, customers who have placed at least 1 order
    customersDF.join(ordersDF, ordersDF("cust_id") === customersDF("customerid"), "semi").write.format("noop").mode("overwrite").save()

    // left anti, customers who haven't placed at least 1 order
    customersDF.join(ordersDF, ordersDF("cust_id") === customersDF("customerid"), "anti").write.format("noop").mode("overwrite").save()

    // hint for shuffle hash join
    ordersDF.join(customersDF.hint("shuffle_hash"), ordersDF("cust_id") === customersDF("customerid"), "anti").write.format("noop").mode("overwrite").save()

    // hint for shuffle short merge join
    ordersDF.join(customersDF.hint("shuffle_merge"), ordersDF("cust_id") === customersDF("customerid"), "anti").write.format("noop").mode("overwrite").save()

    ordersDF.join(customersDF.hint("shuffle_merge"), ordersDF("cust_id") === customersDF("customerid"), "anti").explain()

//    ordersDF.show()
//    customersDF.show()

  }
}
