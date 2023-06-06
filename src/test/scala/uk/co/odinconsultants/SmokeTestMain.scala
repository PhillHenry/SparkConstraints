package uk.co.odinconsultants

import org.apache.spark.sql.DataFrame
import uk.co.odinconsultants.SparkForTesting._

object SmokeTestMain {

  def main(args: Array[String]): Unit = {
    val df: DataFrame = spark.createDataFrame(Seq((1, "a"), (2, "b")))
    df.show()
    df.writeTo("spark_file_test_writeTo").create()
  }

}
