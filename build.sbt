import Dependencies._

ThisBuild / scalaVersion     := "2.11.8" //"2.12.8"
ThisBuild / version          := "1"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

lazy val root = (project in file("."))
  .settings(
    name := "SparkStreamingLogAnalyzer",
    test in assembly := {},
    assemblyMergeStrategy in assembly := {
        case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
        case _ => MergeStrategy.first
    },
    assemblyJarName in assembly := "LogAnalyzer.jar",
    mainClass in assembly := Some("org.sia.loganalyzer.StreamingLogAnalyzer"),
    libraryDependencies += scalaTest % Test,
    libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.4" % "provided",
    libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.4" % "provided",
    libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.4",
    libraryDependencies += "org.apache.kafka" %% "kafka" % "2.3.0"
  )
