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
      "com.github.mjakubowski84" %% "parquet4s-core" % "2.8.0"
    ),

    // Testing dependecies
    libraryDependencies += "org.scalameta" %% "munit" % "1.0.0" % Test
  )
