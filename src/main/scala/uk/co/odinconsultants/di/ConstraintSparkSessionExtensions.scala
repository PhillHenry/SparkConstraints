package uk.co.odinconsultants.di
import org.apache.spark.sql.SparkSessionExtensions

class ConstraintSparkSessionExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    println("TODO")
    extensions.injectPreCBORule { spark => new ReplaceHiveWriter }
  }
}
