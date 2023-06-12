package org.apache.spark.sql.hive

import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.{BucketingUtils, ConstrainedFileFormatWriter}
import org.apache.spark.sql.hive.execution.{DecoratedInsertIntoHiveTable, HiveFileFormat, HiveOptions, SaveAsHiveFile}
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
  override protected def saveAsHiveFile(
      sparkSession: SparkSession,
      plan: SparkPlan,
      hadoopConf: Configuration,
      fileSinkConf: org.apache.spark.sql.hive.HiveShim.ShimFileSinkDesc,
      outputLocation: String,
      customPartitionLocations: Map[TablePartitionSpec, String] = Map.empty,
      partitionAttributes: Seq[Attribute] = Nil,
      bucketSpec: Option[BucketSpec] = None,
  ): Set[String] = {

    val isCompressed =
      fileSinkConf.getTableInfo.getOutputFileFormatClassName.toLowerCase(Locale.ROOT) match {
        case formatName if formatName.endsWith("orcoutputformat") =>
          // For ORC,"mapreduce.output.fileoutputformat.compress",
          // "mapreduce.output.fileoutputformat.compress.codec", and
          // "mapreduce.output.fileoutputformat.compress.type"
          // have no impact because it uses table properties to store compression information.
          false
        case _                                                    => hadoopConf.get("hive.exec.compress.output", "false").toBoolean
      }

    if (isCompressed) {
      hadoopConf.set("mapreduce.output.fileoutputformat.compress", "true")
      fileSinkConf.setCompressed(true)
      fileSinkConf.setCompressCodec(
        hadoopConf
          .get("mapreduce.output.fileoutputformat.compress.codec")
      )
      fileSinkConf.setCompressType(
        hadoopConf
          .get("mapreduce.output.fileoutputformat.compress.type")
      )
    } else {
      // Set compression by priority
      HiveOptions
        .getHiveWriteCompression(fileSinkConf.getTableInfo, sparkSession.sessionState.conf)
        .foreach { case (compression, codec) => hadoopConf.set(compression, codec) }
    }

    val committer = FileCommitProtocol.instantiate(
      sparkSession.sessionState.conf.fileCommitProtocolClass,
      jobId = java.util.UUID.randomUUID().toString,
      outputPath = outputLocation,
    )

    val options = bucketSpec
      .map(_ => Map(BucketingUtils.optionForHiveCompatibleBucketWrite -> "true"))
      .getOrElse(Map.empty)

    ConstrainedFileFormatWriter.write(
      sparkSession = sparkSession,
      plan = plan,
      fileFormat = new HiveFileFormat(fileSinkConf),
      committer = committer,
      outputSpec =
        ConstrainedFileFormatWriter.OutputSpec(outputLocation, customPartitionLocations, outputColumns),
      hadoopConf = hadoopConf,
      partitionColumns = partitionAttributes,
      bucketSpec = bucketSpec,
      statsTrackers = Seq(basicWriteJobStatsTracker(hadoopConf)),
      options = options,
    )
  }
  override def run(
      sparkSession: SparkSession,
      child: SparkPlan,
  ): Seq[Row] = decorated.run(sparkSession, child)
  override protected def withNewChildInternal(
      newChild: LogicalPlan
  ): LogicalPlan = decorated.withNewChildInternal(newChild)
}
