package com.guavus.classwork.week9

object SparkSubmitExample {
  def main(args: Array[String]): Unit = {

  }
}

/*
spark-submit \
  --master yarn \
  --num-executors 3 \
  --executor-cores 4 \
  --executor-memory 2G \
  azure-spark-scala.jar \
  --class com.guavus.classwork.week9.SparkSubmitExample \
  --conf spark.dynamicAllocation.enabled=false \
  --deploy-mode cluster \
  --driver-memory 2G \
  --driver-cores 2 \
  --verbose \

 */

/*
If run on the cluster mode the driver will run on worker node

configuration can be given while creating spark session
Configuration preferences
1. spark session configs
2. spark-submit
3. default config file /etc/spark2/conf/spark-defaults.conf
 */