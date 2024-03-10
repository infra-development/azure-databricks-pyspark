package com.guavus.others

import org.apache.spark.sql.SparkSession
import java.time.LocalDate
import java.time.format.DateTimeFormatter

object InterviewQ2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Adding vacant element with null")
      .master("local[*]")
      .getOrCreate()

    var startDate: LocalDate = LocalDate.parse("2023-01-01")
    val endDate: LocalDate = LocalDate.parse("2024-03-01")
    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM")

    var monthList = collection.mutable.ListBuffer.empty[String]
    while(startDate.isBefore(endDate)) {
      monthList = monthList :+ startDate.format(formatter)
      startDate = startDate.plusMonths(1)
    }

    import spark.implicits._
    val monthDf = monthList.toDF("months")

    monthDf.show()
    val volumeList = List(
      ("2024-02", 100),
      ("2024-01", 50),
      ("2023-09", 200),
      ("2023-04", 100)
    )
    val volumeDF = volumeList.toDF("months_vol", "volume")
    val newDF = monthDf.join(volumeDF, monthDf("months") === volumeDF("months_vol"), "left")
    newDF.select("months", "volume").show()

  }
}
