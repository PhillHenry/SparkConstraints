package org.apache.spark.sql.hive.execution
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

class DecoratedInsertIntoHiveTable(
                                    table: CatalogTable,
                                    partition: Map[String, Option[String]],
                                    query: LogicalPlan,
                                    overwrite: Boolean,
                                    ifPartitionNotExists: Boolean,
                                    outputColumnNames: Seq[String]) extends InsertIntoHiveTable(
  table: CatalogTable,
  partition: Map[String, Option[String]],
  query: LogicalPlan,
  overwrite: Boolean,
  ifPartitionNotExists: Boolean,
  outputColumnNames: Seq[String]) {
  override def withNewChildInternal(newChild: LogicalPlan): InsertIntoHiveTable = super.withNewChildInternal(newChild)
}
