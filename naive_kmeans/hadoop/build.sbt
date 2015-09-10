// Project name (artifact name in Maven)
name := "NaiveKMeansHadoop"

version := "1.0"

// Enables publishing to maven repo
publishMavenStyle := true

// Do not append Scala versions to the generated artifacts
crossPaths := false

// This forbids including Scala related libraries into the dependency
autoScalaLibrary := false

// deps
libraryDependencies += "com.googlecode.json-simple" % "json-simple" % "1.1.1" 
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.6.0" % "provided"