name := "grafomania"

version := "0.1"

scalaVersion := "2.12.11"

val sparkVersion = "3.0.0-preview2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion)

libraryDependencies += "org.opencypher" % "morpheus-spark-cypher" % "0.4.2"