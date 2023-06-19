package uk.co.odinconsultants.di

import org.apache.spark.sql.{DataFrame, Dataset, Row}
import uk.co.odinconsultants.SparkForTesting._

case class Datum(id: Int, label: String)

trait Fixture[T] {
  def data: Seq[T]
}

trait SimpleFixture extends Fixture[Datum] {

  import spark.implicits._

  val IntField: String = "id"

  val tableName: String = this.getClass.getName.replace("$", "_").replace(".", "_")

  val data: Seq[Datum] = Seq(Datum(41, "phill"), Datum(42, "henry"))

  def checkDataIsIn(): Dataset[Datum] = {
    val actual: Dataset[Datum] = spark.read.table(tableName).as[Datum]
    val output: Array[Datum] = actual.collect()
    assert(output.length == data.length)
    assert(output.toSet == data.toSet)
    actual
  }

}
