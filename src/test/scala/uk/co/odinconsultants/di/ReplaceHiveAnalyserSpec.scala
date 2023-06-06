package uk.co.odinconsultants.di
import uk.co.odinconsultants.SparkForTesting._
import org.scalatest.wordspec.AnyWordSpec

class ReplaceHiveAnalyserSpec extends AnyWordSpec {
  "Spark" when {
    "given our configuration" should {
      val df = spark.createDataFrame(Seq((1, "a"), (2, "b")))
      df.show()
      df.writeTo("spark_file_test_writeTo").create()
    }
  }
}
