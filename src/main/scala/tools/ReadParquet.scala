package tools

import org.apache.spark.sql.{SparkSession, DataFrame}
import java.io.File

object ReadParquet {
  def main(args: Array[String]): Unit = {

    if (args.isEmpty) {
      println("Usage: ReadParquet <file> [limit] [path]")
      println("  file: 'dataA', 'dataB', 'output', or a custom filename")
      println("  limit: Number of rows to display (default: 20)")
      println(
        "  path: Custom directory path (default: src/test/resources/test-data/)"
      )
      return
    }

    // Parse arguments
    val filename = args(0)
    val limit =
      if (args.length > 1 && args(1).matches("\\d+")) args(1).toInt else 20
    val customPath = if (args.length > 2) Some(args(2)) else None

    // Determine full path
    val fullPath = if (filename.contains("/") || filename.contains("\\")) {
      // User provided a full path
      filename
    } else {
      // Default directory + filename
      val basePath = customPath.getOrElse("src/test/resources/test-data")
      s"$basePath/$filename"
    }

    println(s"Reading parquet file from: $fullPath")

    // Initialize Spark session
    val spark = SparkSession.builder
      .appName("Parquet Reader")
      .master("local[*]")
      .getOrCreate()

    try {
      // Check if file exists
      val file = new File(fullPath)
      if (!file.exists()) {
        println(s"Error: File or directory not found at $fullPath")
        return
      }

      // Read the Parquet file
      val readParquet = spark.read.parquet(fullPath)

      // Basic information
      println("\n=== BASIC INFORMATION ===")
      println(s"Number of rows: ${readParquet.count()}")
      println(s"Number of columns: ${readParquet.columns.length}")

      // Schema
      println("\n=== SCHEMA ===")
      readParquet.printSchema()

      // Data preview
      println(
        s"\n=== DATA PREVIEW (${Math.min(limit, readParquet.count().toInt)} rows) ==="
      )
      readParquet.show(limit, truncate = false)

      // Determine file type automatically by examining schema
      val fileType = determineFileType(readParquet)

      // Data statistics based on the dataset type
      fileType match {
        case "dataA"  => analyzeDataA(readParquet)
        case "dataB"  => analyzeDataB(readParquet)
        case "output" => analyzeOutput(readParquet)
        case _ =>
          println("\nNo specific analysis available for this file type.")
      }

    } catch {
      case e: Exception =>
        println(s"Error reading parquet file: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

  /** Determine the file type by examining its schema
    */
  def determineFileType(df: DataFrame): String = {
    val columns = df.columns.map(_.toLowerCase).toSet

    if (
      columns.contains("detection_oid") && columns.contains(
        "item_name"
      ) && columns.contains("timestamp_detected")
    ) {
      "dataA"
    } else if (
      columns.contains("geographical_location_oid") && columns.contains(
        "geographical_location"
      ) && columns.size == 2
    ) {
      "dataB"
    } else if (
      columns.contains("geographical_location") && columns.contains(
        "item_rank"
      ) && columns.contains("item_name")
    ) {
      "output"
    } else {
      "unknown"
    }
  }

  def analyzeDataA(df: DataFrame): Unit = {
    println("\n=== DATA A STATISTICS ===")

    // Summary statistics for numeric columns
    println("Summary statistics:")
    df.describe(
      "geographical_location_oid",
      "video_camera_oid",
      "detection_oid",
      "timestamp_detected"
    ).show()

    // Distribution of data by location
    println("Data distribution by geographical location:")
    df.groupBy("geographical_location_oid")
      .count()
      .orderBy("geographical_location_oid")
      .show()

    // Most common items
    println("Most common detected items:")
    val itemCounts = df.groupBy("item_name").count()
    itemCounts.orderBy(itemCounts.col("count").desc).show(10)

    // Duplicate detection_oid count
    val totalRows = df.count()
    val distinctDetections = df.select("detection_oid").distinct().count()
    println(s"Total detection records: $totalRows")
    println(s"Distinct detection_oid count: $distinctDetections")
    println(
      s"Duplicate detection rate: ${(totalRows - distinctDetections) * 100.0 / totalRows}%"
    )
  }

  def analyzeDataB(df: DataFrame): Unit = {
    println("\n=== DATA B STATISTICS ===")

    // Show all locations
    println("All geographical locations:")
    df.select("geographical_location_oid", "geographical_location")
      .orderBy("geographical_location_oid")
      .show(100, truncate = false)
  }

  def analyzeOutput(df: DataFrame): Unit = {
    println("\n=== OUTPUT STATISTICS ===")

    // Show top items by location
    println("Top items by geographical location:")
    df.orderBy("geographical_location", "item_rank")
      .show(100, truncate = false)
  }
}
