# SparkConstraints

Demonstrates data contracts using Apache Spark.
This is purely a very simple Proof of Concept, nothing more.

The contract is stored as metadata.

Spark code has been bastardised to allow the injection of the constraints.
(See the code in `src/main/scala/org/apache/spark/`).
I'm hoping there is a more elegant solution...


# Demonstration

Run the tests with 

`mvn clean install`

You'll see the test `ReplaceHiveAnalyserSpec` run which first persists a table that conforms to the constraints then tries to persist one that violates them. 