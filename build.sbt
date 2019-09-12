import Dependencies._

ThisBuild / scalaVersion     := "2.12.8"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"
ThisBuild / coverageEnabled := true

lazy val root = (project in file("."))
  .settings(
    name := "SparkStreamingLogAnalyzer",
    libraryDependencies += scalaTest % Test,
    test in assembly := {},
    assemblyJarName in assembly := "SparkStreamingLogAnalyzer.jar"
  )

// publishTo := Some(Resolver.file("file", new File("/Users/al/tmp")))
// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
