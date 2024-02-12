package com.guavus.utility

import com.typesafe.config.{Config, ConfigFactory}

object AppConfig {
  val applicationConf: Config = ConfigFactory.load("config.conf")

  // Access configuration values
  val ordersPath: String = applicationConf.getString("files.orders")
  val ordersWhPath: String = applicationConf.getString("files.orders_wh")
  val customersPath: String = applicationConf.getString("files.customers")
  val orderItemPath: String = applicationConf.getString("files.order_items")

  val covid19_cases_path = applicationConf.getString("files.covid19_cases")
  val covid19_states_path = applicationConf.getString("files.covid19_states")
  val student_review_path = applicationConf.getString("files.student_review")
  val groceries_path = applicationConf.getString("files.groceries")
  val products_path = applicationConf.getString("files.products")

}
