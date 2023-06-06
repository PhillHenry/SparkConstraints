package uk.co.odinconsultants.di
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.hive.execution.CreateHiveTableAsSelectCommand

class ReplaceHiveWriter extends Rule[LogicalPlan] with PredicateHelper {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformDown {
    case x @ CreateHiveTableAsSelectCommand(
          tableDesc,
          query,
          outputColumnNames,
          mode
        ) =>
      println(s"x = $x")
      x
  }

}
