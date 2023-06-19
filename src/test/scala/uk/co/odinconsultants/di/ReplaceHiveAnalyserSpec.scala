package uk.co.odinconsultants.di
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{Metadata, MetadataBuilder, StructType}
import org.scalatest.wordspec.AnyWordSpec
import uk.co.odinconsultants.SparkForTesting._

class ReplaceHiveAnalyserSpec extends AnyWordSpec {
  val IntField: String = "id"
  "Spark" when {
    "given our configuration" should {

      val builder                  = new MetadataBuilder
      builder.putLong("max", 2)
      val intMetadata: Metadata    = builder.build()

      "read and write to metastore" in new SimpleFixture {
        val df: DataFrame =
          spark.createDataFrame(data).withColumn(IntField, col(IntField).as(IntField, intMetadata))
        df.writeTo(tableName).create()
        val output        : Dataset[Datum] = checkDataIsIn()
        val outputMetadata: Metadata       = metadataOf(output.schema,  IntField)
        assert(outputMetadata == intMetadata)
        println(outputMetadata.json)

        val schema: StructType = spark.sessionState.catalog.externalCatalog.getTable("default", tableName).schema
        println(s"schema = ${metadataOf(schema, IntField)}")
        assert(outputMetadata == metadataOf(schema, IntField))
      }
    }
  }

  def metadataOf(schema: StructType, colName: String): Metadata = schema.fields.filter(_.name == colName).head.metadata
}
