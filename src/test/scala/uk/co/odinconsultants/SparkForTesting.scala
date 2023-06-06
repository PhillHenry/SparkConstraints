package uk.co.odinconsultants

import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object SparkForTesting {
  val master: String = "local[*]"
  val sparkConf: SparkConf = new SparkConf()
    .setMaster(master)
    .setAppName("Tests")
    .set("spark.driver.allowMultipleContexts", "true")
    .set(CATALOG_IMPLEMENTATION.key, "hive")
  sparkConf.set("spark.driver.allowMultipleContexts", "true")
  val sc: SparkContext = SparkContext.getOrCreate(sparkConf)
  val spark: SparkSession = SparkSession.builder().getOrCreate()
  val sqlContext: SQLContext = spark.sqlContext

}
