name := "NaiveKmeans"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.4.1" % "provided"
libraryDependencies += "org.scalanlp" %% "breeze" % "0.11.2"
libraryDependencies += "org.scalanlp" %% "breeze-natives" % "0.11.2"


resolvers += "Akka Repository" at "http://repo.akka.io/releases/"