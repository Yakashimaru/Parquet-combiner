// HTX Data Engineering Assessment
// Created by: Joel John Tan
// Date: March 2025

package tools

import org.apache.spark.sql.{SparkSession, DataFrame}
import java.io.File
import com.htx.utils.Logging

object ReadParquet extends Logging {
  private val defaultNoRows = 20
  private val maxNoRows = 100
  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      printUsage()
    } else {
      // Parse arguments
      val (filename, limit, customPath) = parseCommandLineArgs(args)

      // Determine full path
      val fullPath = determineFullPath(filename, customPath)
      logger.info(s"Reading parquet file from: $fullPath")

      // Initialize Spark session
      val spark = initializeSparkSession()

      try {
        processParquetFile(spark, fullPath, limit)
      } catch {
        case e: Exception =>
          logger.info(s"Error reading parquet file: ${e.getMessage}")
          e.printStackTrace()
      } finally {
        spark.stop()
      }
    }
  }

  private def printUsage(): Unit = {
    logger.error("Usage: ReadParquet <file> [limit] [path]")
    logger.error("  file: 'dataA', 'dataB', 'output', or a custom filename")
    logger.error("  limit: Number of rows to display (default: 20)")
    logger.error(
      "  path: Custom directory path (default: src/test/resources/test-data/)"
    )
  }

  private def parseCommandLineArgs(
      args: Array[String]
  ): (String, Int, Option[String]) = {
    val filename = args(0)
    val limit =
      if (args.length > 1 && args(1).matches("\\d+")) {
        args(1).toInt
      } else {
        defaultNoRows
      }
    val customPath = if (args.length > 2) Some(args(2)) else None

    (filename, limit, customPath)
  }

  private def determineFullPath(
      filename: String,
      customPath: Option[String]
  ): String = {
    if (filename.contains("/") || filename.contains("\\")) {
      // User provided a full path
      filename
    } else {
      // Default directory + filename
      val basePath = customPath.getOrElse("src/test/resources/test-data")
      s"$basePath/$filename"
    }
  }

  private def initializeSparkSession(): SparkSession = {
    SparkSession
      .builder()
      .appName("Parquet Reader")
      .master("local[*]")
      .getOrCreate()
  }

  private def processParquetFile(
      spark: SparkSession,
      fullPath: String,
      limit: Int
  ): Unit = {
    // Check if file exists
    val file = new File(fullPath)
    if (!file.exists()) {
      logger.error(s"Error: File or directory not found at $fullPath")
    } else {
      // Read the Parquet file
      val readParquet = spark.read.parquet(fullPath)

      // Display basic information
      displayBasicInfo(readParquet, limit)

      // Determine file type and perform analysis
      val fileType = determineFileType(readParquet)
      analyzeByFileType(readParquet, fileType)
    }
  }

  private def displayBasicInfo(df: DataFrame, limit: Int): Unit = {
    // Basic information
    logger.info("\n=== BASIC INFORMATION ===")
    logger.info(s"Number of rows: ${df.count()}")
    logger.info(s"Number of columns: ${df.columns.length}")

    // Schema
    logger.info("\n=== SCHEMA ===")
    df.printSchema()

    // Data preview
    logger.info(
      s"\n=== DATA PREVIEW (${Math.min(limit, df.count().toInt)} rows) ==="
    )
    df.show(limit, truncate = false)
  }

  private def analyzeByFileType(df: DataFrame, fileType: String): Unit = {
    fileType match {
      case "dataA"  => analyzeDataA(df)
      case "dataB"  => analyzeDataB(df)
      case "output" => analyzeOutput(df)
      case _ =>
        logger.info("\nNo specific analysis available for this file type.")
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
    logger.info("\n=== DATA A STATISTICS ===")

    // Summary statistics for numeric columns
    logger.info("Summary statistics:")
    df.describe(
      "geographical_location_oid",
      "video_camera_oid",
      "detection_oid",
      "timestamp_detected"
    ).show()

    // Distribution of data by location
    logger.info("Data distribution by geographical location:")
    df.groupBy("geographical_location_oid")
      .count()
      .orderBy("geographical_location_oid")
      .show()

    // Most common items
    logger.info("Most common detected items:")
    val itemCounts = df.groupBy("item_name").count()
    itemCounts.orderBy(itemCounts.col("count").desc).show(defaultNoRows)

    // Duplicate detection_oid count
    val totalRows = df.count()
    val distinctDetections = df.select("detection_oid").distinct().count()
    logger.info(s"Total detection records: $totalRows")
    logger.info(s"Distinct detection_oid count: $distinctDetections")
    logger.info(
      s"Duplicate detection rate: ${(totalRows - distinctDetections) * 100.0 / totalRows}%"
    )
  }

  def analyzeDataB(df: DataFrame): Unit = {
    logger.info("\n=== DATA B STATISTICS ===")

    // Show all locations
    logger.info("All geographical locations:")
    df.select("geographical_location_oid", "geographical_location")
      .orderBy("geographical_location_oid")
      .show(maxNoRows, truncate = false)
  }

  def analyzeOutput(df: DataFrame): Unit = {
    logger.info("\n=== OUTPUT STATISTICS ===")

    // Show top items by location
    logger.info("Top items by geographical location:")
    df.orderBy("geographical_location", "item_rank")
      .show(maxNoRows, truncate = false)
  }
}
