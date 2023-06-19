package uk.co.odinconsultants.di
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{Metadata, MetadataBuilder, StructType}
import org.scalatest.wordspec.AnyWordSpec
import uk.co.odinconsultants.SparkForTesting._
import uk.co.odinconsultants.di.ConstraintLittleLanguage.KEY_MAX_INT

class ReplaceHiveAnalyserSpec extends AnyWordSpec {

  "Spark" when {
    "using our bastardised code" should {

      "successfully store the constraints as metadata" in new SimpleFixture {
        val intMetadata: Metadata    = maxInt(data.map(_.id).max)
        val df: DataFrame            =
          spark.createDataFrame(data).withColumn(IntField, col(IntField).as(IntField, intMetadata))
        df.writeTo(tableName).create()
        val output: Dataset[Datum]   = checkDataIsIn()
        val outputMetadata: Metadata = metadataOf(output.schema, IntField)
        assert(outputMetadata == intMetadata)
        val schema: StructType       =
          spark.sessionState.catalog.externalCatalog.getTable("default", tableName).schema
        assert(outputMetadata == metadataOf(schema, IntField))
      }

      "nothing is written and an exception is thrown  if a constraint is violated" in new SimpleFixture {
        val intMetadata: Metadata    = maxInt(data.map(_.id).min - 1)
        val df: DataFrame =
          spark.createDataFrame(data).withColumn(IntField, col(IntField).as(IntField, intMetadata))
        assertThrows[Exception] {
          df.writeTo(tableName).create()
        }
        assertThrows[NoSuchTableException] {
          spark.sessionState.catalog.externalCatalog.getTable("default", tableName)
        }
      }
    }
  }

  private def maxInt(maxInt: Int): Metadata = {
    val builder = new MetadataBuilder
    builder.putLong(KEY_MAX_INT, maxInt)
    builder.build()
  }

  def metadataOf(schema: StructType, colName: String): Metadata =
    schema.fields.filter(_.name == colName).head.metadata
}
