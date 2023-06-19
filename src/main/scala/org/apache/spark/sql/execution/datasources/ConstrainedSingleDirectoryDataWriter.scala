package org.apache.spark.sql.execution.datasources
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.Metadata
import uk.co.odinconsultants.di.ConstraintLittleLanguage.KEY_MAX_INT

class ConstrainedSingleDirectoryDataWriter(
    description: WriteJobDescription,
    taskAttemptContext: TaskAttemptContext,
    committer: FileCommitProtocol,
    customMetrics: Map[String, SQLMetric] = Map.empty,
) extends SingleDirectoryDataWriter(description, taskAttemptContext, committer, customMetrics) {

  override def write(record: InternalRow): Unit = {

    description.allColumns.zipWithIndex.foreach { case (attr, index) =>
      val metadata: Metadata = attr.metadata
      if (metadata.contains(KEY_MAX_INT)) {
        val actual: Int = record.getInt(index)
        val max: Long   = metadata.getLong(KEY_MAX_INT)
        if (actual > max) throw new Exception(s"$actual > $max for ${attr.name}")
      }
    }

    super.write(record)
  }
}
