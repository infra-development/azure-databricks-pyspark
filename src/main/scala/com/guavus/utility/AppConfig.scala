package com.guavus.utility

import com.typesafe.config.{Config, ConfigFactory}

object AppConfig {
  val applicationConf: Config = ConfigFactory.load("config.conf")

  // Access configuration values
  val ordersPath: String = applicationConf.getString("files.orders")
  val ordersWhPath: String = applicationConf.getString("files.orders_wh")
  val ordersSamplePath: String = applicationConf.getString("files.orders_sample")
  val ordersSample1Path: String = applicationConf.getString("files.orders_sample1")
  val ordersSample2Path: String = applicationConf.getString("files.orders_sample2")
  val ordersSample3Path: String = applicationConf.getString("files.orders_sample3")
  val customersPath: String = applicationConf.getString("files.customers")
  val customerNestedPath: String = applicationConf.getString("files.customer_nested")
  val orderItemPath: String = applicationConf.getString("files.order_items")
  val covid19_cases_path = applicationConf.getString("files.covid19_cases")
  val covid19_states_path = applicationConf.getString("files.covid19_states")
  val student_review_path = applicationConf.getString("files.student_review")
  val groceries_path = applicationConf.getString("files.groceries")
  val products_path = applicationConf.getString("files.products")
  val library_path = applicationConf.getString("files.library")
  val train_path = applicationConf.getString("files.train")
  val sales_data = applicationConf.getString("files.sales_data")
  val hospital_data = applicationConf.getString("files.hospital")
  val order_data = applicationConf.getString("files.order_data")
  val window_data = applicationConf.getString("files.windowdata")
  val window_data_modfied = applicationConf.getString("files.windowdata_modified")

}
