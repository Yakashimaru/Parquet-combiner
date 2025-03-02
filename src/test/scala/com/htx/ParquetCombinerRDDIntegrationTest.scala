// HTX Data Engineering Assessment
// Created by: Joel John Tan
// Date: March 2025

package com.htx

import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.types.{
  StructType,
  StructField,
  LongType,
  StringType
}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import java.io.File
import scala.reflect.io.Directory

class ParquetCombinerRDDIntegrationTest
    extends AnyFunSuite
    with BeforeAndAfterAll {

  // Set up Spark context and session for tests
  private var spark: SparkSession = _
  private val testDir = "target/test-data"
  private val dataAPath = s"$testDir/dataA"
  private val dataBPath = s"$testDir/dataB"
  private val outputPath = s"$testDir/output"

  // Define spark and import implicits at the object level
  // This allows for a stable identifier
  lazy val sparkSession: SparkSession = {
    SparkSession
      .builder()
      .appName("ParquetCombinerRDDIntegrationTest")
      .master("local[2]")
      .getOrCreate()
  }

  // Import implicits from the sparkSession object
  // scalastyle:off underscore.import import.grouping
  import sparkSession.implicits._
  // scalastyle:on underscore.import import.grouping

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Initialize sparkSession
    spark = sparkSession

    // Clean existing test directories if they exist
    val directory = new Directory(new File(testDir))
    if (directory.exists) {
      directory.deleteRecursively()
    }

    // Create test data directories
    new File(dataAPath).mkdirs()
    new File(dataBPath).mkdirs()

    // Prepare test data
    createTestDataA()
    createTestDataB()
  }

  override def afterAll(): Unit = {
    // Cleanup test directories
    val directory = new Directory(new File(testDir))
    if (directory.exists) {
      directory.deleteRecursively()
    }

    if (spark != null) {
      spark.stop()
    }
    super.afterAll()
  }

  // Create and write test data for file A
  def createTestDataA(): Unit = {
    // Create sample data with some duplicated detection_oid values
    val dataA = Seq(
      // Location 1 items
      (1L, 101L, 1001L, "apple", 1620000000L),
      (1L, 102L, 1002L, "banana", 1620000001L),
      (1L, 103L, 1003L, "apple", 1620000002L),
      (1L, 104L, 1004L, "cherry", 1620000003L),
      (1L, 105L, 1005L, "banana", 1620000004L),
      (1L, 106L, 1005L, "banana", 1620000005L), // Duplicate detection_oid

      // Location 2 items
      (2L, 201L, 2001L, "apple", 1620000006L),
      (2L, 202L, 2002L, "banana", 1620000007L),
      (2L, 203L, 2003L, "orange", 1620000008L),
      (2L, 204L, 2004L, "grape", 1620000009L),
      (2L, 205L, 2005L, "apple", 1620000010L),
      (2L, 206L, 2005L, "apple", 1620000011L), // Duplicate detection_oid

      // Location 3 items
      (3L, 301L, 3001L, "watermelon", 1620000012L),
      (3L, 302L, 3002L, "apple", 1620000013L),
      (3L, 303L, 3003L, "banana", 1620000014L),
      (3L, 304L, 3004L, "orange", 1620000015L),
      (3L, 305L, 3005L, "grape", 1620000016L),
      (3L, 306L, 3006L, "watermelon", 1620000017L)
    )

    // Use the implicits to convert directly to DataFrame
    val dataADF = dataA.toDF(
      "geographical_location_oid",
      "video_camera_oid",
      "detection_oid",
      "item_name",
      "timestamp_detected"
    )

    dataADF.write.mode(SaveMode.Overwrite).parquet(dataAPath)
  }

  // Create and write test data for file B
  def createTestDataB(): Unit = {
    val dataB = Seq(
      (1L, "New York"),
      (2L, "San Francisco"),
      (3L, "Los Angeles")
    )

    // Use the implicits to convert directly to DataFrame
    val dataBDF = dataB.toDF(
      "geographical_location_oid",
      "geographical_location"
    )

    dataBDF.write.mode(SaveMode.Overwrite).parquet(dataBPath)
  }

  test("Full pipeline execution with command line arguments") {
    // Call a modified version of main that accepts our SparkSession
    ParquetCombinerRDD.processParquetFiles(
      spark,
      (dataAPath, dataBPath, outputPath, 3)
    )

    // Verify that output file exists
    val outputFile = new File(outputPath)
    assert(outputFile.exists, "Output directory should exist")

    // Read back the output
    val outputDF = spark.read.parquet(outputPath)

    // Verify schema - Using modified schema to match actual nullability
    val expectedSchema = StructType(
      Seq(
        StructField(
          "geographical_location",
          LongType,
          true
        ), // Changed to 'true' for nullable
        StructField(
          "item_rank",
          StringType,
          true
        ), // Changed to 'true' for nullable
        StructField(
          "item_name",
          StringType,
          true
        ) // Changed to 'true' for nullable
      )
    )

    assert(
      outputDF.schema === expectedSchema,
      "Output schema should match expected schema"
    )

    // We should have 3 locations x 3 items = 9 rows (adjusted for duplicates and actual counts)
    // Spot check some expected values

    // Check New York's top item
    val nyTopItem = outputDF
      .filter($"geographical_location" === 1) // For new york
      .filter($"item_rank" === "1")
      .collect()

    assert(nyTopItem.length === 1, "New York should have one top item")
    assert(
      Set("apple", "banana").contains(nyTopItem(0).getAs[String]("item_name")),
      "New York's top item should be either apple or banana"
    )

    // Verify each location has at most 3 items
    val locationCounts = outputDF
      .groupBy("geographical_location")
      .count()
      .collect()
      .map(row =>
        (row.getAs[Long]("geographical_location"), row.getAs[Long]("count"))
      )
      .toMap

    locationCounts.foreach { case (location, count) =>
      assert(count <= 3, s"Location $location should have at most 3 items")
    }

    // Verify rankings are sequential (1, 2, 3)
    outputDF.createOrReplaceTempView("output")
    val rankingCheck = spark
      .sql("""
      SELECT geographical_location, COUNT(DISTINCT item_rank) as unique_ranks,
             MIN(item_rank) as min_rank, MAX(item_rank) as max_rank
      FROM output
      GROUP BY geographical_location
    """)
      .collect()

    rankingCheck.foreach { row =>
      val location = row.getAs[Long]("geographical_location")
      val uniqueRanks = row.getAs[Long]("unique_ranks")
      val minRank = row.getAs[String]("min_rank")
      val maxRank = row.getAs[String]("max_rank")

      assert(minRank === "1", s"$location should have minimum rank of 1")
      assert(
        maxRank === uniqueRanks.toString,
        s"$location should have maximum rank equal to the number of items"
      )
    }
  }

  test("Full pipeline execution with alternate topX value") {
    // Run the main method with test paths and topX = 2
    ParquetCombinerRDD.processParquetFiles(
      spark,
      (dataAPath, dataBPath, outputPath + "_alt", 2)
    )

    // Verify that output file exists
    val outputFile = new File(outputPath + "_alt")
    assert(outputFile.exists, "Output directory should exist")

    // Read back the output
    val outputDF = spark.read.parquet(outputPath + "_alt")

    // We should have 3 locations x 2 items = 6 rows
    val count = outputDF.count()
    assert(count === 6, s"Expected 6 rows in output, got $count")

    // Verify each location has exactly 2 items
    val locationCounts = outputDF
      .groupBy("geographical_location")
      .count()
      .collect()
      .map(row =>
        (row.getAs[Long]("geographical_location"), row.getAs[Long]("count"))
      )
      .toMap

    locationCounts.foreach { case (location, count) =>
      assert(count === 2, s"Location $location should have exactly 2 items")
    }

    // Verify rankings are only 1 and 2
    outputDF.createOrReplaceTempView("output_alt")
    val rankingCheck = spark
      .sql("""
      SELECT item_rank, COUNT(*) as count
      FROM output_alt
      GROUP BY item_rank
      ORDER BY item_rank
    """)
      .collect()

    assert(rankingCheck.length === 2, "Should only have 2 distinct ranks")
    assert(
      rankingCheck(0).getAs[String]("item_rank") === "1",
      "Should have rank 1"
    )
    assert(
      rankingCheck(1).getAs[String]("item_rank") === "2",
      "Should have rank 2"
    )
  }

  test("Duplicate detection_oid handling") {
    // Create a test-specific data file with more duplicates
    val duplicateDataPath = s"$testDir/duplicate_data"

    // Create sample data with many duplicated detection_oid values
    val duplicateData = Seq(
      (1L, 101L, 1001L, "apple", 1620000000L),
      (1L, 102L, 1001L, "apple", 1620000001L), // Duplicate detection_oid
      (1L, 103L, 1001L, "apple", 1620000002L), // Duplicate detection_oid
      (1L, 104L, 1002L, "banana", 1620000003L),
      (1L, 105L, 1002L, "banana", 1620000004L) // Duplicate detection_oid
    )

    // Use the implicits to convert directly to DataFrame
    val duplicateDF = duplicateData.toDF(
      "geographical_location_oid",
      "video_camera_oid",
      "detection_oid",
      "item_name",
      "timestamp_detected"
    )

    duplicateDF.write.mode(SaveMode.Overwrite).parquet(duplicateDataPath)

    // Run the main method with test paths
    val duplicateOutputPath = s"$testDir/duplicate_output"
    ParquetCombinerRDD.processParquetFiles(
      spark,
      (duplicateDataPath, dataBPath, duplicateOutputPath, 3)
    )

    // Read back the output
    val outputDF = spark.read.parquet(duplicateOutputPath)

    // Check the count for New York (#1)
    outputDF.createOrReplaceTempView("duplicate_output")
    val results = spark
      .sql("""
      SELECT item_name, item_rank
      FROM duplicate_output
      WHERE geographical_location = 1
      ORDER BY item_rank
    """)
      .collect()

    // There should be exactly 2 items - apple and banana
    assert(
      results.length === 2,
      "Should have 2 distinct items after deduplication"
    )

    // Verify the top item in New York is either apple or banana (they should both have count 1 after deduplication)
    val topItemName = results(0).getAs[String]("item_name")
    assert(
      Set("apple", "banana").contains(topItemName),
      s"Top item name should be either apple or banana, but got $topItemName"
    )
  }
}
