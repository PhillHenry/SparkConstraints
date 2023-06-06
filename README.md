# SparkConstraints

`CreateHiveTableAsSelectCommand.run` creates a `InsertIntoHiveTable` (which is a `DataWritingCommand`)
This is all called when the lazy `QueryExecution.executedPlan` is referenced.
It's instantiated in `HiveAnalysis.apply`, a `Rule[LogicalPlan]`.
`HiveAnalysis` is added to `Analyser.postHocResolutionRules`. The Analyzer in turn is created in `BaseSessionStateBuilder.analyzer`
Which `BaseSessionStateBuilder` we use (see `SparkSession.sessionState` which uses the conf) seems limited to two options (see `SparkSession.sessionStateClassName`)

`SingleDirectoryDataWriter.write` which is a `FileFormatDataWriter` has access to the column metadata via `dscription.allColumns.AttributeReference.metaData`
`SingleDirectoryDataWriter` is instantiated in `FileFormatWriter.executeTask` which in turn is called from `SaveAsHiveFile.saveAsHiveFile` (specifically, its sub class `InsertIntoHiveTable`) which in turn springs out of `CreateHiveTableAsSelectCommand`
`CreateHiveTableAsSelectCommand` is instantiated in cloning the lazy `commandExecuted` `LogicalPlan` in `QueryExecution`.
Note that executeTask runs on the executor
Note that `FileFormatWriter.write` is part run in driver and executor, the latter running the thunk in `sparkSession.sparkContext.runJob` that was created by the driver springing from `DataWritingCommandExec.executeCollect`. This includes calls to `LocalTableScanExec.doExecute` but it's just putting together a data structure for the executor. Its RDDs `map` function is  called on the executor.

LocalRelation is created springing from SparkSession.createdDataFrame

SparkStrategies.BasicOperators is called by `QueryExecution.executePhase`
