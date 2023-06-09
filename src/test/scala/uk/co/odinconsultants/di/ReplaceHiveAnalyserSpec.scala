package uk.co.odinconsultants.di
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{IntegerType, Metadata, MetadataBuilder, StringType, StructField, StructType}
import uk.co.odinconsultants.SparkForTesting._
import org.scalatest.wordspec.AnyWordSpec
import org.apache.spark.sql.functions._

class ReplaceHiveAnalyserSpec extends AnyWordSpec {
  "Spark" when {
    "given our configuration" should {
      "read and write to metastore" in {
        val data: Seq[(Int, String)] = Seq((1, "a"), (2, "b"))
        val builder = new MetadataBuilder
        builder.putLong("max", 2)

        val intField: StructField = StructField("an_int", IntegerType, false, builder.build())
        val struct: StructType  = StructType(
          Seq(
            intField,
            StructField("a_string", StringType, false, Metadata.empty),
          )
        )
        val colName            = "_1"
        val df    : DataFrame   = spark.createDataFrame(data).withColumn(colName, col(colName).as(colName, builder.build()))
        val tableName           = "spark_file_test_writeTo"
        df.show()
        df.writeTo(tableName).create()
        val output: DataFrame        = spark.read.table(tableName)
        assert(output.collect().length == data.length)
      }
    }
  }
}
