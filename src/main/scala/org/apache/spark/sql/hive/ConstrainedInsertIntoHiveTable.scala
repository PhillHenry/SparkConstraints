package org.apache.spark.sql.hive

import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.{BucketingUtils, ConstrainedFileFormatWriter}
import org.apache.spark.sql.hive.execution.{
  DecoratedInsertIntoHiveTable,
  HiveFileFormat,
  HiveOptions,
  SaveAsHiveFile,
}
import org.apache.spark.sql.{Row, SparkSession}

import java.util.Locale

case class ConstrainedInsertIntoHiveTable(
    table: CatalogTable,
    partition: Map[String, Option[String]],
    query: LogicalPlan,
    overwrite: Boolean,
    ifPartitionNotExists: Boolean,
    outputColumnNames: Seq[String],
) extends SaveAsHiveFile {
  val decorated = new DecoratedInsertIntoHiveTable(
    table,
    partition,
    query,
    overwrite,
    ifPartitionNotExists,
    outputColumnNames,
  )
  override def run(
      sparkSession: SparkSession,
      child: SparkPlan,
  ): Seq[Row] = decorated.run(sparkSession, child)
  override protected def withNewChildInternal(
      newChild: LogicalPlan
  ): LogicalPlan = decorated.withNewChildInternal(newChild)
}
