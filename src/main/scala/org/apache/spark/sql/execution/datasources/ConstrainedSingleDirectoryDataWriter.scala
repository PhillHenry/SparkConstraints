package org.apache.spark.sql.execution.datasources
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.execution.metric.SQLMetric

class ConstrainedSingleDirectoryDataWriter(
    description: WriteJobDescription,
    taskAttemptContext: TaskAttemptContext,
    committer: FileCommitProtocol,
    customMetrics: Map[String, SQLMetric] = Map.empty,
) extends SingleDirectoryDataWriter(description, taskAttemptContext, committer, customMetrics) {}
