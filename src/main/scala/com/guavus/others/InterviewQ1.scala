package com.guavus.others

import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.functions._

// read one csv file in spark having 100 distinct entry of name,
// save 80 entries of file to 80 different csv file each csv filename should be name of entry.

object InterviewQ1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkCSVProcessing")
      .master("local[*]")  // You can adjust the master URL based on your Spark setup
      .getOrCreate()

    val inputFile = "path/to/your/inputfile.csv"
    val df = spark.read.option("header", "true").csv(inputFile)

    // Extract 80 distinct entries from the 'name' column
    val distinctEntries = df.select("name").distinct().limit(80)

    // Iterate over each distinct entry and save it to a separate CSV file
    distinctEntries.collect().foreach((row: Row) => {
      val entryName: String = row.getString(0)
      val outputFile: String = s"path/to/your/output/directory/$entryName.csv"

      // Filter the DataFrame for the specific entry and save it to a CSV file
      df.filter(col("name") === entryName)
        .coalesce(1)  // Optionally, coalesce to a single partition for a single output file
        .write
        .option("header", "true")
        .csv(outputFile)
    })

  }
}
