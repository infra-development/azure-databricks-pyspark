package com.guavus.utility

import com.typesafe.config.{Config, ConfigFactory}

object AppConfig {
  val applicationConf: Config = ConfigFactory.load("config.conf")

  // Access configuration values
  val ordersPath: String = applicationConf.getString("files.orders")
}
