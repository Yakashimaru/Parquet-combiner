import sbt.Keys._

val scala2Version = "2.13.16"
val sparkVersion = "3.5.4"

lazy val root = project
  .in(file("."))
  .settings(
    name := "HTX_Data_Engineer_Test",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala2Version,

    // For java 17 compatibility
    fork := true,
    javaOptions ++= Seq(
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED"
    ),
    
    // Control SBT logging more aggressively
    logLevel := Level.Error,
    showSuccess := false,
    suppressSbtShellNotification := true,
    
    // Redirect Spark output
    outputStrategy := Some(StdoutOutput),
    
    // Disable all extra output
    traceLevel := -1,

    // Spark dependencies
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "com.github.mjakubowski84" %% "parquet4s-core" % "2.8.0",
      "org.apache.parquet" % "parquet-hadoop" % "1.12.3",
      "org.apache.hadoop" % "hadoop-client" % "3.3.4" % "provided"
    ),

    // Testing dependencies - updating to include ScalaTest for unit and integration tests
    libraryDependencies ++= Seq(
      "org.scalameta" %% "munit" % "1.0.0" % Test,
      "org.scalatest" %% "scalatest" % "3.2.15" % Test,
      "org.mockito" % "mockito-core" % "4.11.0" % Test
    ),

    // Custom task to run with minimal logging
    commands += Command.command("runQuiet") { state =>
      "set outputStrategy := Some(StdoutOutput)" ::
      "set logLevel := Level.Error" ::
      "set traceLevel := -1" ::
      "runMain ParquetCombinerRDD" ::
      state
    },
    
    // ScalaStyle configuration
    scalastyleConfig := baseDirectory.value / "scalastyle-config.xml",
    
    // Main class for assembly
    Compile / mainClass := Some("ParquetCombinerRDD"),
    
    // Assembly merge strategy for creating a fat JAR
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case "log4j.properties" => MergeStrategy.first
      case x => MergeStrategy.first
    },
    
    // Additional compiler options
    scalacOptions ++= Seq(
      "-deprecation", 
      "-feature",
      "-unchecked",
      "-Xlint",
      "-Wdead-code",
      "-Wunused:imports"
    ),
    
    // Ensure tests run in sequence (helpful for Spark tests)
    Test / parallelExecution := false,

    // Add the ScalaStyle plugin settings
    scalastyleFailOnError := true,
    scalastyleFailOnWarning := false,
    
    // Add scalastyle to test - proper way to handle InputKey
    Test / test := {
      (Test / scalastyle).toTask("").value
      (Test / test).value
    },

    Test / testOptions += Tests.Argument("-oS") // S for silent
  )