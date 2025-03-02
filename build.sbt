import sbt.Keys._

val scala2Version = "2.12.18"
val sparkVersion = "3.5.4"

// Define custom run tasks for different logging levels
lazy val quietRun = taskKey[Unit]("Runs the application with minimal logging")

lazy val root = project
  .in(file("."))
  .settings(
    name := "HTX_Data_Engineer_Test",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala2Version,

    // For java 17 compatibility
    fork := true,
    outputStrategy := Some(StdoutOutput),
    javaOptions ++= Seq(
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED"
    ),

    // Spark dependencies
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "com.github.mjakubowski84" %% "parquet4s-core" % "2.8.0",
      "org.apache.parquet" % "parquet-hadoop" % "1.12.3",
      "org.apache.hadoop" % "hadoop-client" % "3.3.4" % "provided",
      "org.slf4j" % "slf4j-api" % "1.7.36",
      "org.slf4j" % "slf4j-log4j12" % "1.7.36"
    ),

    // Testing dependencies - updating to include ScalaTest for unit and integration tests
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.15" % Test,
      "org.mockito" % "mockito-core" % "4.11.0" % Test
    ),

    // Custom run tasks for different logging levels
    quietRun := {
      (Compile / run).toTask(" -Dlog4j.configuration=log4j-quiet.properties").value
    },
    
    // Ensure log4j configuration files are included in resources
    Compile / unmanagedResources / includeFilter := "*.properties",

    // Custom command to run with minimal logging 
    commands += Command.single("quietRunMain") { (state, mainClass) =>
      s"""set javaOptions += "-Dlog4j.configuration=log4j-quiet.properties"""" ::
      s"runMain $mainClass" ::
      state
    },
      
    // ScalaStyle configuration
    scalastyleConfig := baseDirectory.value / "scalastyle-config.xml",
    
    // Main class for assembly
    Compile / mainClass := Some("com.htx.ParquetCombinerRDD"),
    
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
    ),
    
    // Ensure tests run in sequence 
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