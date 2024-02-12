package com.guavus.assignments.week4

import com.guavus.utility.AppConfig
import org.apache.spark.sql.SparkSession

object W4Question2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Week4 Question2")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    val cases_rdd = spark.sparkContext.textFile(AppConfig.covid19_cases_path)
    val states_rdd = spark.sparkContext.textFile(AppConfig.covid19_states_path)

    /*
    Cases

    date(0) || state(1) || positive(2) || negative(3) || pending(4) || hospitalizedCurrently(5) || hospitalizedCumulative(6) || inIcuCurrently(7) || inIcuCumulative(8) || onVentilatorCurrently(9) || onVentilatorCumulative(10) || recovered(11) || dataQualityGrade(12) || lastUpdateEt(13) || dateModified(14) || checkTimeEt(15) || death(16) || hospitalized(17) || dateChecked(18) || totalTestsViral(19) || positiveTestsViral(20) || negativeTestsViral(21) || positiveCasesViral(22) || deathConfirmed(23) || deathProbable(24) || fips(25) || positiveIncrease(26) || negativeIncrease(27) || total(28) || totalTestResults(29) || totalTestResultsIncrease(30) || posNeg(31) || deathIncrease(32) || hospitalizedIncrease(33) || hash(34) || commercialScore(35) || negativeRegularScore(36) || negativeScore(37) || positiveScore(38) || score(39) || grade(40) ||

    states

    state(0) || notes(1) || covid19Site(2) || covid19SiteSecondary(3) || covid19SiteTertiary(4) || twitter(5) || covid19SiteOld(6) || name(7) || fips(8) || pui(9) || pum(10) ||

     */

    /*
    val cases_str = "state,notes,covid19Site,covid19SiteSecondary,covid19SiteTertiary,twitter,covid19SiteOld,name,fips,pui,pum"

    val zipped_cases: Array[(String, Int)] = cases_str.split(",").zipWithIndex
    for(i <- zipped_cases) {
      print(s"${i._1}(${i._2}) || ")
    }
     */

    // 1. Find the top 10 states with the highest no of positive cases
    val state_positive = cases_rdd.map(x => (x.split(",")(1), x.split(",")(2).toInt))
    state_positive.reduceByKey((x, y) => x + y).sortBy(x => x._2, ascending = false).take(10).foreach(println)

    // 2. Find the total count of people currently in ICU
    println(cases_rdd.map(x => x.split(",")(7).toInt).sum())

    // 3.Find the top 15 States having maximum no. of recovery.
    println("Find the top 15 States having maximum no. of recovery")
    val state_recovery = cases_rdd.map(x => (x.split(",")(1), x.split(",")(11).toInt))
    state_recovery.reduceByKey((x, y) =>x + y).sortBy(x => x._2, ascending = false).take(15).foreach(println)

    // 4.Find the top 3 States having least no. of deaths.
    println("Find the top 3 States having least no. of deaths")
    val state_death = cases_rdd.map(x => (x.split(",")(1), x.split(",")(23).toInt))
    state_death.reduceByKey((x, y) =>x + y).sortBy(x => x._2).take(3).foreach(println)

    // 5.Find the total number of people hospitalized currently.
    print("Hospitalized : --> " + cases_rdd.map(x => x.split(",")(5).toInt).sum())

    // 6.List the twitter handle and fips code for the top 15 states with the highest number of total cases.
    val state_cases = cases_rdd.map(x => {
      val fields = x.split(",")
      (fields(1), fields(28).toInt)
    })
    val state_total_cases = state_cases.reduceByKey((x, y) => x + y)

    val state_twitter_fips = states_rdd.map(x => {
      val fields = x.split(",")
      (fields(0), (fields(5), fields(8)))
    })

    println("-----------(state, (cases, (twitter, fips)))---------------")
    val state_twitter_fips_total = state_total_cases.join(state_twitter_fips)
    state_twitter_fips_total.sortBy(x => x._2._1, ascending = false).take(10).foreach(println)

  }

}
