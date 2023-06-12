package org.apache.spark.sql.hive

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.ql.plan.FileSinkDesc
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.hive.execution.{DecoratedInsertIntoHiveTable, InsertIntoHiveTable, SaveAsHiveFile}
import org.apache.spark.sql.{Row, SparkSession}

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
  override protected def saveAsHiveFile(
      sparkSession: SparkSession,
      plan: SparkPlan,
      hadoopConf: Configuration,
      fileSinkConf: org.apache.spark.sql.hive.HiveShim.ShimFileSinkDesc,
      outputLocation: String,
      customPartitionLocations: Map[TablePartitionSpec, String] = Map.empty,
      partitionAttributes: Seq[Attribute] = Nil,
      bucketSpec: Option[BucketSpec] = None,
  ): Set[String] = super.saveAsHiveFile(sparkSession, plan, hadoopConf, fileSinkConf, outputLocation, customPartitionLocations, partitionAttributes, bucketSpec)
  override def run(
      sparkSession: SparkSession,
      child: SparkPlan,
  ): Seq[Row] = decorated.run(sparkSession, child)
  override protected def withNewChildInternal(
      newChild: LogicalPlan
  ): LogicalPlan = decorated.withNewChildInternal(newChild)
}
