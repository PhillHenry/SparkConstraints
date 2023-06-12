package org.apache.spark.sql.execution.datasources
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.metric.SQLMetric

class ConstrainedSingleDirectoryDataWriter(
    description: WriteJobDescription,
    taskAttemptContext: TaskAttemptContext,
    committer: FileCommitProtocol,
    customMetrics: Map[String, SQLMetric] = Map.empty,
) extends SingleDirectoryDataWriter(description, taskAttemptContext, committer, customMetrics) {
  println("ConstrainedSingleDirectoryDataWriter")
  override def write(record: InternalRow): Unit = {
    // TODO this line will explode for anything but the test. But it demonstrates that we can make assertions here
    println(s"record = ${record.getInt(0)} ${record.getString(1)}, allColumns = ${description.allColumns.head.metadata}, dataColumns = ${description.dataColumns}")
    super.write(record)
  }
}
