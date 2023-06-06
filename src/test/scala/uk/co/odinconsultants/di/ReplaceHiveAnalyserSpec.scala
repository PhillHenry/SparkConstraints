package uk.co.odinconsultants.di
import org.apache.spark.sql.DataFrame
import uk.co.odinconsultants.SparkForTesting._
import org.scalatest.wordspec.AnyWordSpec

class ReplaceHiveAnalyserSpec extends AnyWordSpec {
  "Spark" when {
    "given our configuration" should {
      "read and write to metastore" in {
        val data: Seq[(Int, String)] = Seq((1, "a"), (2, "b"))
        val df: DataFrame            = spark.createDataFrame(data)
        val tableName                = "spark_file_test_writeTo"
        df.show()
        df.writeTo(tableName).create()
        val output: DataFrame        = spark.read.table(tableName)
        assert(output.collect().length == data.length)
      }
    }
  }
}
