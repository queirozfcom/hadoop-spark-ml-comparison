name := "CleanDataToSequenceFile"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "joda-time" % "joda-time" % "2.8.2"
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.4.1" % "provided"


resolvers += "Akka Repository" at "http://repo.akka.io/releases/"