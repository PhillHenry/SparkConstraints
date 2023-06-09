package uk.co.odinconsultants.di
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{Metadata, MetadataBuilder}
import org.scalatest.wordspec.AnyWordSpec
import uk.co.odinconsultants.SparkForTesting._

class ReplaceHiveAnalyserSpec extends AnyWordSpec {
  "Spark" when {
    "given our configuration" should {
      "read and write to metastore" in {
        val data: Seq[(Int, String)] = Seq((1, "a"), (2, "b"))
        val builder                  = new MetadataBuilder
        builder.putLong("max", 2)
        val intMetadata: Metadata    = builder.build()
        val colName                  = "_1"
        val df: DataFrame            =
          spark.createDataFrame(data).withColumn(colName, col(colName).as(colName, intMetadata))
        val tableName                = "spark_file_test_writeTo"
        df.show()
        df.writeTo(tableName).create()
        val output: DataFrame        = spark.read.table(tableName)
        assert(output.collect().length == data.length)
        assert(output.schema.fields.filter(_.name == colName).head.metadata == intMetadata)
      }
    }
  }
}
