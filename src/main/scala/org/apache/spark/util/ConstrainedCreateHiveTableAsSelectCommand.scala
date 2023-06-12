package org.apache.spark.util

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, SessionCatalog}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.DataWritingCommand
import org.apache.spark.sql.hive.ConstrainedInsertIntoHiveTable
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectBase, CreateHiveTableAsSelectCommand, InsertIntoHiveTable}

case class ConstrainedCreateHiveTableAsSelectCommand(
    tableDesc: CatalogTable,
    query: LogicalPlan,
    outputColumnNames: Seq[String],
    mode: SaveMode,
) extends CreateHiveTableAsSelectBase {

  override def getWritingCommand(
      catalog: SessionCatalog,
      tableDesc: CatalogTable,
      tableExists: Boolean,
  ): DataWritingCommand = {
    // For CTAS, there is no static partition values to insert.
    val partition = tableDesc.partitionColumnNames.map(_ -> None).toMap
    ConstrainedInsertIntoHiveTable(
      tableDesc,
      partition,
      query,
      overwrite = if (tableExists) false else true,
      ifPartitionNotExists = false,
      outputColumnNames = outputColumnNames,
    )
  }

  override def writingCommandClassName: String =
    Utils.getSimpleName(classOf[ConstrainedInsertIntoHiveTable])

  override protected def withNewChildInternal(
      newChild: LogicalPlan
  ): ConstrainedCreateHiveTableAsSelectCommand = copy(query = newChild)
}
