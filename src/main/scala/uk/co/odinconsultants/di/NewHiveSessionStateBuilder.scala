package uk.co.odinconsultants.di
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.{BaseSessionStateBuilder, SessionStateBuilder}

class NewHiveSessionStateBuilder(session: SparkSession,
                                 unused: Option[Any])
  extends BaseSessionStateBuilder(session, None) {
  override protected def newBuilder: NewBuilder = new NewHiveSessionStateBuilder(_, _)

}
