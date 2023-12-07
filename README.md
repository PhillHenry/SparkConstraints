# SparkConstraints

Demonstrates data contracts using Apache Spark.
This is a very simple Proof of Concept, nothing more.

The contract is stored in the metadata of a `DataFrame` in the metastore.

Spark code has been bastardised to allow the injection of the constraints.
(See the code in `src/main/scala/org/apache/spark/`).
I'm hoping there is a more elegant solution...


# Demonstration

Run the tests with 

`mvn test`

You'll see the test [ReplaceHiveAnalyserSpec](./uk/co/odinconsultants/di/ReplaceHiveAnalyserSpec.scala) 
run which first persists a table that conforms to the constraints then tries to persist one that violates them. 