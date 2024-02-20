package com.guavus.assignments.week6

import com.guavus.utility.AppConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, to_date}
import org.apache.spark.sql.types.StructType

object W6Question5 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Week6 Question5")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    val hospital_data_path = AppConfig.hospital_data
    val schemaString = "patient_id integer, admission_date string, discharge_date string, diagnosis string, doctor_id integer, total_cost float"
    val schema = StructType.fromDDL(schemaString)

    val df = spark.read.schema(schema).option("header", "true").csv(hospital_data_path)
    df.show()
    df.printSchema()

    // 1. Drop doctor_id column from dataset
    val df1 = df.drop("doctor_id")

    // 2. Rename the "total_cost column to "hospital_bill"
    val df2 = df1.withColumnRenamed("total_cost", "hospital_bill")

    // 3 Add a new column called "duration_of_stay" that represents the number of days
    // a patient stayed in hospital. (Hint: the duration should be calculated as the difference between the "discharge_date" and "admission_date" columns.

    val df3 = df2.withColumn("admission_date", to_date(col("admission_date"), "MM-dd-yyyy")).withColumn("discharge_date", to_date(col("discharge_date"), "yyyy-MM-dd"))

    val df4 = df3.withColumn("duration_of_stay", expr("datediff(discharge_date, admission_date)"))
    df4.show()

    /*
    4. Create a new column called "adjusted_total_cost" that calculates the adjusted total cost based on the diagnosis as follows:
    If the diagnosis is "Heart Attack", multiply the hospital_bill by 1.5.
    If the diagnosis is "Appendicitis", multiply the hospital_bill by 1.2.
    For any other diagnosis, keep the hospital_bill as it is.
     */

    val df5 = df4.withColumn("adjusted_total_cost", expr("CASE WHEN diagnosis = 'Heart Attack' THEN hospital_bill * 1.5 WHEN diagnosis = 'Appendicitis' THEN hospital_bill * 1.2 ELSE hospital_bill END"))
    df5.select("patient_id", "diagnosis", "hospital_bill", "adjusted_total_cost").show()

  }

}
