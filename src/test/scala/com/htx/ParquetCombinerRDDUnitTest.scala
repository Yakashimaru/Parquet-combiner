// HTX Data Engineering Assessment
// Created by: Joel John Tan
// Date: March 2025

package com.htx

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.apache.log4j.{Level, Logger}

// Import the appropriate classes from your new structure
import com.htx.models.Models._
import com.htx.services._
import com.htx.utils._

class ParquetCombinerRDDUnitTest extends AnyFunSuite with BeforeAndAfterAll {

  // Set up Spark context and session for tests
  private var sc: SparkContext = _
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Create test Spark context and session
    val conf = new SparkConf()
      .setAppName("ParquetCombinerRDDUnitTest")
      .setMaster("local[2]")
    sc = new SparkContext(conf)
    spark = SparkSession.builder().config(sc.getConf).getOrCreate()
  }

  override def afterAll(): Unit = {
    if (sc != null) {
      sc.stop()
    }
    if (spark != null) {
      spark.stop()
    }
    super.afterAll()
  }

  // Helper method to create test data
  private def createTestDataA(
      testCases: Seq[(Long, Long, Long, String, Long)]
  ): RDD[DataA] = {
    sc.parallelize(testCases.map { case (loc, camera, detect, item, time) =>
      DataA(loc, camera, detect, item, time)
    })
  }

  // Helper method to create location data
  private def createTestDataB(testCases: Seq[(Long, String)]): RDD[DataB] = {
    sc.parallelize(testCases.map { case (id, name) =>
      DataB(id, name)
    })
  }

  // Test that directly uses the TopItemsAggregation
  test("TopItemsAggregation should deduplicate and join correctly") {
    // Create test data with duplicated detection_oid
    val dataA = createTestDataA(
      Seq(
        (1L, 101L, 1001L, "item1", 1620000000L),
        (1L, 102L, 1001L, "item1", 1620000001L), // Duplicate detection_oid
        (2L, 103L, 1002L, "item2", 1620000002L)
      )
    )

    val dataB = createTestDataB(
      Seq(
        (1L, "Location A"),
        (2L, "Location B")
      )
    )

    // Use the TopItemsAggregation directly
    val topItemsAggregation = new Aggregations.TopItemsAggregation()
    val params = Map("topX" -> 1)
    val result = topItemsAggregation.aggregate(dataA, dataB, params).collect()

    // Verify results
    assert(result.length === 2, "Should have 2 items (one from each location)")

    // Verify location mapping
    val locationAResults = result.filter(_.geographical_location_oid == 1L)
    val locationBResults = result.filter(_.geographical_location_oid == 2L)

    assert(locationAResults.length === 1, "Location A should have 1 item")
    assert(locationBResults.length === 1, "Location B should have 1 item")

    // Verify the items match
    assert(
      locationAResults.head.item_name == "item1",
      "Location A should have item1"
    )
    assert(
      locationBResults.head.item_name == "item2",
      "Location B should have item2"
    )
  }

  // We can test the implicit ranking functionality in TopItemsAggregation
  test("TopItemsAggregation should sort and rank items correctly") {
    // Test data with different item counts
    val testData = Seq(
      // Location 1 has: 3 item1, 2 item2, 1 item3
      (1L, 101L, 1001L, "item1", 1620000000L),
      (1L, 102L, 1002L, "item1", 1620000001L),
      (1L, 103L, 1003L, "item1", 1620000002L),
      (1L, 104L, 1004L, "item2", 1620000003L),
      (1L, 105L, 1005L, "item2", 1620000004L),
      (1L, 106L, 1006L, "item3", 1620000005L)
    )

    val dataA = createTestDataA(testData)
    val dataB = createTestDataB(Seq((1L, "Location A")))

    // Get top 3 items using TopItemsAggregation
    val topItemsAggregation = new Aggregations.TopItemsAggregation()
    val params = Map("topX" -> 3)
    val results = topItemsAggregation.aggregate(dataA, dataB, params).collect()

    // Verify results
    assert(results.length === 3, "Should return exactly 3 items")

    // Sort by rank to verify ordering
    val sortedResults = results.sortBy(_.item_rank)

    // Verify ranking order based on count: item1(3) > item2(2) > item3(1)
    assert(
      sortedResults(0).item_name === "item1" && sortedResults(
        0
      ).item_rank === "1",
      "First rank should be item1"
    )
    assert(
      sortedResults(1).item_name === "item2" && sortedResults(
        1
      ).item_rank === "2",
      "Second rank should be item2"
    )
    assert(
      sortedResults(2).item_name === "item3" && sortedResults(
        2
      ).item_rank === "3",
      "Third rank should be item3"
    )

    // Verify all items have the correct location
    assert(
      results.forall(_.geographical_location_oid === 1L),
      "All results should have the correct location"
    )
  }

  test(
    "TopItemsAggregation should correctly handle deduplicated detection_oid"
  ) {
    // Create test data with duplicated detection_oid
    val dataA = createTestDataA(
      Seq(
        (1L, 101L, 1001L, "item1", 1620000000L),
        (1L, 102L, 1001L, "item1", 1620000001L), // Duplicate detection_oid
        (2L, 103L, 1002L, "item2", 1620000002L),
        (2L, 104L, 1003L, "item3", 1620000003L),
        (3L, 105L, 1004L, "item1", 1620000004L),
        (3L, 106L, 1005L, "item2", 1620000005L)
      )
    )

    val dataB = createTestDataB(
      Seq(
        (1L, "Location A"),
        (2L, "Location B"),
        (3L, "Location C")
      )
    )

    // Process the test data using TopItemsAggregation
    val topX = 2 // Get top 2 items per location
    val topItemsAggregation = new Aggregations.TopItemsAggregation()
    val params = Map("topX" -> topX)
    val result = topItemsAggregation.aggregate(dataA, dataB, params).collect()

    // Verify results

    // Check total result count: 3 locations x up to 2 items per location
    // But Location A has only one unique item after deduplication, so total should be 5
    assert(result.length === 5, "Should have 5 items total after deduplication")

    // Verify Location A has only one item (item1) after deduplication
    val locationAResults = result.filter(_.geographical_location_oid == 1L)
    assert(locationAResults.length === 1, "Location A should have 1 item")
    assert(
      locationAResults.exists(r =>
        r.item_name == "item1" && r.item_rank == "1"
      ),
      "Location A should have item1 with rank 1"
    )

    // Verify Location B has two items (item2 and item3)
    val locationBResults = result.filter(_.geographical_location_oid == 2L)
    assert(locationBResults.length === 2, "Location B should have 2 items")

    // Sort the results by rank for verification
    val sortedLocationBResults = locationBResults.sortBy(_.item_rank)
    assert(
      sortedLocationBResults(0).item_rank == "1" && sortedLocationBResults(
        1
      ).item_rank == "2",
      "Location B should have items with ranks 1 and 2"
    )

    // Verify Location C has two items (item1 and item2)
    val locationCResults = result.filter(_.geographical_location_oid == 3L)
    assert(locationCResults.length === 2, "Location C should have 2 items")

    // Sort the results by rank for verification
    val sortedLocationCResults = locationCResults.sortBy(_.item_rank)
    assert(
      sortedLocationCResults(0).item_rank == "1" && sortedLocationCResults(
        1
      ).item_rank == "2",
      "Location C should have items with ranks 1 and 2"
    )
  }

  test("TopItemsAggregation should correctly rank items by count") {
    // Create test data with different item counts
    val dataA = createTestDataA(
      Seq(
        // Location 1 has: 3 item1, 2 item2, 1 item3
        (1L, 101L, 1001L, "item1", 1620000000L),
        (1L, 102L, 1002L, "item1", 1620000001L),
        (1L, 103L, 1003L, "item1", 1620000002L),
        (1L, 104L, 1004L, "item2", 1620000003L),
        (1L, 105L, 1005L, "item2", 1620000004L),
        (1L, 106L, 1006L, "item3", 1620000005L),

        // Location 2 has: 1 item1, 3 item2, 2 item3
        (2L, 201L, 2001L, "item1", 1620000006L),
        (2L, 202L, 2002L, "item2", 1620000007L),
        (2L, 203L, 2003L, "item2", 1620000008L),
        (2L, 204L, 2004L, "item2", 1620000009L),
        (2L, 205L, 2005L, "item3", 1620000010L),
        (2L, 206L, 2006L, "item3", 1620000011L)
      )
    )

    val dataB = createTestDataB(
      Seq(
        (1L, "Location A"),
        (2L, "Location B")
      )
    )

    // Process the test data using TopItemsAggregation
    val topX = 3 // Get top 3 items per location
    val topItemsAggregation = new Aggregations.TopItemsAggregation()
    val params = Map("topX" -> topX)
    val result = topItemsAggregation.aggregate(dataA, dataB, params).collect()

    // Verify results

    // Check total result count: 2 locations x 3 items per location = 6 results
    assert(result.length === 6, "Should have 6 items total")

    // Verify Location A ranking: item1 (rank 1), item2 (rank 2), item3 (rank 3)
    val locationAResults = result
      .filter(_.geographical_location_oid == 1L)
      .sortBy(_.item_rank)
    assert(locationAResults.length === 3, "Location A should have 3 items")
    assert(
      locationAResults(0).item_name === "item1",
      "Location A rank 1 should be item1"
    )
    assert(
      locationAResults(1).item_name === "item2",
      "Location A rank 2 should be item2"
    )
    assert(
      locationAResults(2).item_name === "item3",
      "Location A rank 3 should be item3"
    )

    // Verify Location B ranking: item2 (rank 1), item3 (rank 2), item1 (rank 3)
    val locationBResults = result
      .filter(_.geographical_location_oid == 2L)
      .sortBy(_.item_rank)
    assert(locationBResults.length === 3, "Location B should have 3 items")
    assert(
      locationBResults(0).item_name === "item2",
      "Location B rank 1 should be item2"
    )
    assert(
      locationBResults(1).item_name === "item3",
      "Location B rank 2 should be item3"
    )
    assert(
      locationBResults(2).item_name === "item1",
      "Location B rank 3 should be item1"
    )
  }

  test("TopItemsAggregation should respect the topX parameter") {
    // Create test data with various item counts
    val dataA = createTestDataA(
      Seq(
        // Location has 5 different items
        (1L, 101L, 1001L, "item1", 1620000000L),
        (1L, 102L, 1002L, "item2", 1620000001L),
        (1L, 103L, 1003L, "item3", 1620000002L),
        (1L, 104L, 1004L, "item4", 1620000003L),
        (1L, 105L, 1005L, "item5", 1620000004L)
      )
    )

    val dataB = createTestDataB(
      Seq(
        (1L, "Location A")
      )
    )

    // Test with topX = 2
    val topX = 2
    val topItemsAggregation = new Aggregations.TopItemsAggregation()
    val params = Map("topX" -> topX)
    val result = topItemsAggregation.aggregate(dataA, dataB, params).collect()

    // Verify results
    // Should only return 2 results for Location A
    assert(result.length === 2, "Should have 2 items when topX = 2")

    // Check that ranks are correct
    val ranks = result.map(_.item_rank).sorted
    assert(ranks.sameElements(Array("1", "2")), "Ranks should be 1 and 2")

    // Test with topX = 4
    val topX2 = 4
    val params2 = Map("topX" -> topX2)
    val result2 = topItemsAggregation.aggregate(dataA, dataB, params2).collect()

    // Verify results
    // Should return 4 results for Location A
    assert(result2.length === 4, "Should have 4 items when topX = 4")

    // Check that ranks are correct
    val ranks2 = result2.map(_.item_rank).sorted
    assert(
      ranks2.sameElements(Array("1", "2", "3", "4")),
      "Ranks should be 1, 2, 3, and 4"
    )
  }

  test("TopItemsAggregation should handle empty input correctly") {
    // Create empty test data
    val emptyDataA = sc.parallelize(Seq.empty[DataA])
    val dataB = createTestDataB(
      Seq(
        (1L, "Location A"),
        (2L, "Location B")
      )
    )

    // Process the test data using TopItemsAggregation
    val topX = 3
    val topItemsAggregation = new Aggregations.TopItemsAggregation()
    val params = Map("topX" -> topX)
    val result =
      topItemsAggregation.aggregate(emptyDataA, dataB, params).collect()

    // Verify results
    // Should return empty result
    assert(result.isEmpty, "Should return empty result for empty input")
  }

  // Test the AggregationFactory
  test("AggregationFactory should create the correct aggregation operations") {
    // Test creating topItems aggregation
    val topItemsAgg = AggregationFactory.createAggregation("topItems")
    assert(
      topItemsAgg.isInstanceOf[Aggregations.TopItemsAggregation],
      "Should create TopItemsAggregation"
    )

    // Test creating itemCount aggregation
    val itemCountAgg = AggregationFactory.createAggregation("itemCount")
    assert(
      itemCountAgg.isInstanceOf[Aggregations.ItemCountAggregation],
      "Should create ItemCountAggregation"
    )

    // Test creating locationStats aggregation
    val locationStatsAgg = AggregationFactory.createAggregation("locationStats")
    assert(
      locationStatsAgg.isInstanceOf[Aggregations.LocationStatsAggregation],
      "Should create LocationStatsAggregation"
    )

    // Test invalid aggregation type
    assertThrows[IllegalArgumentException] {
      AggregationFactory.createAggregation("invalidType")
    }
  }

  // Test ItemCountAggregation
  test("ItemCountAggregation should count items correctly") {
    // Create test data
    val dataA = createTestDataA(
      Seq(
        (1L, 101L, 1001L, "item1", 1620000000L),
        (1L, 102L, 1002L, "item1", 1620000001L),
        (1L, 103L, 1003L, "item2", 1620000002L),
        (2L, 201L, 2001L, "item1", 1620000003L),
        (2L, 202L, 2001L, "item1", 1620000004L) // Duplicate detection_oid
      )
    )

    val dataB = createTestDataB(
      Seq(
        (1L, "Location A"),
        (2L, "Location B")
      )
    )

    // Use ItemCountAggregation
    val itemCountAggregation = new Aggregations.ItemCountAggregation()
    val result =
      itemCountAggregation.aggregate(dataA, dataB, Map.empty).collect()

    // Verify results
    assert(result.length === 3, "Should have 3 item count results")

    // Location 1, item1 count should be 2
    val location1Item1 = result.find(r =>
      r.geographical_location_oid == 1L && r.item_name == "item1"
    )
    assert(location1Item1.isDefined, "Should have result for Location 1, item1")
    assert(location1Item1.get.count == 2, "Location 1, item1 count should be 2")

    // Location 1, item2 count should be 1
    val location1Item2 = result.find(r =>
      r.geographical_location_oid == 1L && r.item_name == "item2"
    )
    assert(location1Item2.isDefined, "Should have result for Location 1, item2")
    assert(location1Item2.get.count == 1, "Location 1, item2 count should be 1")

    // Location 2, item1 count should be 1 (after deduplication)
    val location2Item1 = result.find(r =>
      r.geographical_location_oid == 2L && r.item_name == "item1"
    )
    assert(location2Item1.isDefined, "Should have result for Location 2, item1")
    assert(
      location2Item1.get.count == 1,
      "Location 2, item1 count should be 1 after deduplication"
    )
  }

  // Test LocationStatsAggregation
  test(
    "LocationStatsAggregation should calculate location statistics correctly"
  ) {
    // Create test data
    val dataA = createTestDataA(
      Seq(
        // Location 1: 3 detections, 2 unique items, camera 101 most active (2 detections)
        (1L, 101L, 1001L, "item1", 1620000000L),
        (1L, 101L, 1002L, "item2", 1620000001L),
        (1L, 102L, 1003L, "item1", 1620000002L),

        // Location 2: 2 detections, 1 unique item, camera 201 most active (2 detections)
        (2L, 201L, 2001L, "item3", 1620000003L),
        (2L, 201L, 2002L, "item3", 1620000004L)
      )
    )

    val dataB = createTestDataB(
      Seq(
        (1L, "Location A"),
        (2L, "Location B")
      )
    )

    // Use LocationStatsAggregation
    val locationStatsAggregation = new Aggregations.LocationStatsAggregation()
    val result =
      locationStatsAggregation.aggregate(dataA, dataB, Map.empty).collect()

    // Verify results
    assert(result.length === 2, "Should have 2 location stats results")

    // Verify Location 1 stats
    val location1Stats = result.find(_.geographical_location_oid == 1L)
    assert(location1Stats.isDefined, "Should have stats for Location 1")
    assert(
      location1Stats.get.total_detections == 3,
      "Location 1 should have 3 total detections"
    )
    assert(
      location1Stats.get.unique_items == 2,
      "Location 1 should have 2 unique items"
    )
    assert(
      location1Stats.get.most_active_camera == 101L,
      "Location 1's most active camera should be 101"
    )

    // Verify Location 2 stats
    val location2Stats = result.find(_.geographical_location_oid == 2L)
    assert(location2Stats.isDefined, "Should have stats for Location 2")
    assert(
      location2Stats.get.total_detections == 2,
      "Location 2 should have 2 total detections"
    )
    assert(
      location2Stats.get.unique_items == 1,
      "Location 2 should have 1 unique item"
    )
    assert(
      location2Stats.get.most_active_camera == 201L,
      "Location 2's most active camera should be 201"
    )
  }

  // Test for data skew handling
  test("SkewHandler should process skewed data correctly") {
    // Create test data with skew - location 1 has many more detections than others
    val dataA = createTestDataA(
      Seq(
        // Location 1 has a lot of items (simulating data skew)
        (1L, 101L, 1001L, "item1", 1620000000L),
        (1L, 102L, 1002L, "item1", 1620000001L),
        (1L, 103L, 1003L, "item1", 1620000002L),
        (1L, 104L, 1004L, "item2", 1620000003L),
        (1L, 105L, 1005L, "item2", 1620000004L),
        (1L, 106L, 1006L, "item3", 1620000005L),
        (1L, 107L, 1007L, "item3", 1620000006L),
        (1L, 108L, 1008L, "item4", 1620000007L),
        (1L, 109L, 1009L, "item4", 1620000008L),
        (1L, 110L, 1010L, "item5", 1620000009L),

        // Other locations have few items
        (2L, 201L, 2001L, "item1", 1620000010L),
        (3L, 301L, 3001L, "item1", 1620000011L)
      )
    )

    // Process skewed data with SkewHandler
    val skewedLocationId = 1L
    val numPartitions = 4
    val result = SkewedDataHandler
      .processSkewedData(dataA, skewedLocationId, numPartitions)
      .collect()

    // Verify results
    // Check that location 1 items were processed
    val location1Results = result.filter(_._1 == 1L)
    assert(location1Results.nonEmpty, "Should have results for skewed location")

    // Check item counts for location 1
    val itemCounts = location1Results.groupBy(_._2).mapValues(_.size)
    assert(
      itemCounts("item1") == 1,
      "item1 should have count 1 after processing"
    )
    assert(
      itemCounts("item2") == 1,
      "item2 should have count 1 after processing"
    )
    assert(
      itemCounts("item3") == 1,
      "item3 should have count 1 after processing"
    )
    assert(
      itemCounts("item4") == 1,
      "item4 should have count 1 after processing"
    )
    assert(
      itemCounts("item5") == 1,
      "item5 should have count 1 after processing"
    )

    // Verify other locations were processed correctly
    val location2Results = result.filter(_._1 == 2L)
    val location3Results = result.filter(_._1 == 3L)
    assert(location2Results.length == 1, "Location 2 should have 1 result")
    assert(location3Results.length == 1, "Location 3 should have 1 result")
  }

  // Test for empty geographical locations
  test(
    "TopItemsAggregation should handle geographical locations with no items"
  ) {
    // Create test data where location 2 has no items
    val dataA = createTestDataA(
      Seq(
        (1L, 101L, 1001L, "item1", 1620000000L),
        (1L, 102L, 1002L, "item2", 1620000001L),
        (3L, 301L, 3001L, "item3", 1620000002L)
        // Location 2 has no items
      )
    )

    val dataB = createTestDataB(
      Seq(
        (1L, "Location A"),
        (2L, "Location B"), // This location has no items in dataA
        (3L, "Location C")
      )
    )

    // Use TopItemsAggregation
    val topItemsAggregation = new Aggregations.TopItemsAggregation()
    val params = Map("topX" -> 3)
    val result = topItemsAggregation.aggregate(dataA, dataB, params).collect()

    // Verify results
    val location1Results = result.filter(_.geographical_location_oid == 1L)
    val location2Results = result.filter(_.geographical_location_oid == 2L)
    val location3Results = result.filter(_.geographical_location_oid == 3L)

    assert(location1Results.nonEmpty, "Location 1 should have results")
    assert(location2Results.isEmpty, "Location 2 should have no results")
    assert(location3Results.nonEmpty, "Location 3 should have results")
  }

  // Test for very large topX values
  test("TopItemsAggregation should handle topX larger than available items") {
    // Create test data with limited items
    val dataA = createTestDataA(
      Seq(
        (1L, 101L, 1001L, "item1", 1620000000L),
        (1L, 102L, 1002L, "item2", 1620000001L),
        (1L, 103L, 1003L, "item3", 1620000002L)
      )
    )

    val dataB = createTestDataB(
      Seq(
        (1L, "Location A")
      )
    )

    // Use TopItemsAggregation with topX larger than available items
    val topItemsAggregation = new Aggregations.TopItemsAggregation()
    val params = Map("topX" -> 10) // Only 3 items available
    val result = topItemsAggregation.aggregate(dataA, dataB, params).collect()

    // Verify results
    assert(
      result.length == 3,
      "Should only return as many items as are available"
    )

    // Check ranks are still sequential
    val ranks = result.map(_.item_rank).sorted
    assert(
      ranks.sameElements(Array("1", "2", "3")),
      "Ranks should be 1, 2, and 3"
    )
  }

  // Test for null or malformed values
  test("Aggregations should handle null or empty item names") {
    // Create test data with null and empty item names
    val dataA = createTestDataA(
      Seq(
        (1L, 101L, 1001L, "item1", 1620000000L),
        (1L, 102L, 1002L, "", 1620000001L), // Empty item name
        (1L, 103L, 1003L, null, 1620000002L), // Null item name
        (2L, 201L, 2001L, "item2", 1620000003L)
      )
    )

    val dataB = createTestDataB(
      Seq(
        (1L, "Location A"),
        (2L, "Location B")
      )
    )

    // Use TopItemsAggregation
    val topItemsAggregation = new Aggregations.TopItemsAggregation()
    val params = Map("topX" -> 3)
    val result = topItemsAggregation.aggregate(dataA, dataB, params).collect()

    // Verify results
    val location1Results = result.filter(_.geographical_location_oid == 1L)
    assert(
      location1Results.length == 3,
      "Location 1 should have 3 results including null and empty"
    )

    // Check that null and empty items are included
    val itemNames = location1Results.map(_.item_name).toSet
    assert(itemNames.contains("item1"), "Should include 'item1'")
    assert(itemNames.contains(""), "Should include empty string")
    assert(itemNames.contains(null), "Should include null")
  }
}
