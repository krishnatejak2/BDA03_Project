name := "BDA03-pipeline"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.2"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.6.2" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.6.2"

libraryDependencies += "log4j" % "log4j" % "1.2.14"
libraryDependencies += "joda-time" % "joda-time" % "2.8.1"

