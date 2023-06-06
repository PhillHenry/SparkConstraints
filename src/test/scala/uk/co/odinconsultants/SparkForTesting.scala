package uk.co.odinconsultants

import org.apache.spark.sql.internal.StaticSQLConf.{
  CATALOG_IMPLEMENTATION,
  SPARK_SESSION_EXTENSIONS,
  WAREHOUSE_PATH,
}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import uk.co.odinconsultants.di.ConstraintSparkSessionExtensions

import java.io.File
import java.nio.file.Files

object SparkForTesting {
  val master: String         = "local[*]"
  val sparkConf: SparkConf   = {
    val dir: String = Files.createTempDirectory("SparkForTesting").toString
    println(s"Using temp directory $dir")
    new SparkConf()
      .setMaster(master)
      .setAppName("Tests")
      .set("spark.driver.allowMultipleContexts", "true")
      .set(CATALOG_IMPLEMENTATION.key, "hive")
      .set(WAREHOUSE_PATH.key, dir)
      .set("METASTOREWAREHOUSE", dir)
      .set(SPARK_SESSION_EXTENSIONS.key, classOf[ConstraintSparkSessionExtensions].getCanonicalName)
  }
  sparkConf.set("spark.driver.allowMultipleContexts", "true")
  val sc: SparkContext       = SparkContext.getOrCreate(sparkConf)
  val spark: SparkSession    = SparkSession.builder().getOrCreate()
  val sqlContext: SQLContext = spark.sqlContext

}
