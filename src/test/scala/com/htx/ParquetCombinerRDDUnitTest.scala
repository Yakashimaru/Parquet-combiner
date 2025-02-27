package com.htx

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.apache.log4j.{Level, Logger}

class ParquetCombinerRDDUnitTest extends AnyFunSuite with BeforeAndAfterAll {
  
  // Set up Spark context and session for tests
  private var sc: SparkContext = _
  private var spark: SparkSession = _
  
  override def beforeAll(): Unit = {
    super.beforeAll()
    // Silence logging for cleaner test output
    Logger.getRootLogger.setLevel(Level.ERROR)
    
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
  
  test("processRDDs should correctly handle deduplicated detection_oid") {
    // Create test data with duplicated detection_oid
    val dataA = sc.parallelize(Seq(
      ParquetCombinerRDD.DataA(1L, 101L, 1001L, "item1", 1620000000L),
      ParquetCombinerRDD.DataA(1L, 102L, 1001L, "item1", 1620000001L),  // Duplicate detection_oid
      ParquetCombinerRDD.DataA(2L, 103L, 1002L, "item2", 1620000002L),
      ParquetCombinerRDD.DataA(2L, 104L, 1003L, "item3", 1620000003L),
      ParquetCombinerRDD.DataA(3L, 105L, 1004L, "item1", 1620000004L),
      ParquetCombinerRDD.DataA(3L, 106L, 1005L, "item2", 1620000005L)
    ))
    
    val dataB = sc.parallelize(Seq(
      ParquetCombinerRDD.DataB(1L, "Location A"),
      ParquetCombinerRDD.DataB(2L, "Location B"),
      ParquetCombinerRDD.DataB(3L, "Location C")
    ))
    
    // Process the test data
    val topX = 2  // Get top 2 items per location
    val result = ParquetCombinerRDD.processRDDs(dataA, dataB, topX).collect()
    
    // Verify results
    
    // Check total result count: 3 locations x 2 items per location = 6 results (minus duplicates)
    // But Location A has only one unique item after deduplication, so total should be 5
    assert(result.length === 5)
    
    // Verify Location A has only one item (item1) after deduplication
    val locationAResults = result.filter(_.geographical_location == "Location A")
    assert(locationAResults.length === 1)
    assert(locationAResults.exists(r => r.item_name == "item1" && r.item_rank == "1"))
    
    // Verify Location B has two items (item2 and item3)
    val locationBResults = result.filter(_.geographical_location == "Location B")
    assert(locationBResults.length === 2)
    assert(locationBResults.exists(r => r.item_name == "item2" && r.item_rank == "1") || 
           locationBResults.exists(r => r.item_name == "item2" && r.item_rank == "2"))
    assert(locationBResults.exists(r => r.item_name == "item3" && r.item_rank == "1") || 
           locationBResults.exists(r => r.item_name == "item3" && r.item_rank == "2"))
    
    // Verify Location C has two items (item1 and item2)
    val locationCResults = result.filter(_.geographical_location == "Location C")
    assert(locationCResults.length === 2)
    assert(locationCResults.exists(r => r.item_name == "item1" && r.item_rank == "1") || 
           locationCResults.exists(r => r.item_name == "item1" && r.item_rank == "2"))
    assert(locationCResults.exists(r => r.item_name == "item2" && r.item_rank == "1") || 
           locationCResults.exists(r => r.item_name == "item2" && r.item_rank == "2"))
  }
  
  test("processRDDs should correctly rank items by count") {
    // Create test data with different item counts
    val dataA = sc.parallelize(Seq(
      // Location 1 has: 3 item1, 2 item2, 1 item3
      ParquetCombinerRDD.DataA(1L, 101L, 1001L, "item1", 1620000000L),
      ParquetCombinerRDD.DataA(1L, 102L, 1002L, "item1", 1620000001L),
      ParquetCombinerRDD.DataA(1L, 103L, 1003L, "item1", 1620000002L),
      ParquetCombinerRDD.DataA(1L, 104L, 1004L, "item2", 1620000003L),
      ParquetCombinerRDD.DataA(1L, 105L, 1005L, "item2", 1620000004L),
      ParquetCombinerRDD.DataA(1L, 106L, 1006L, "item3", 1620000005L),
      
      // Location 2 has: 1 item1, 3 item2, 2 item3
      ParquetCombinerRDD.DataA(2L, 201L, 2001L, "item1", 1620000006L),
      ParquetCombinerRDD.DataA(2L, 202L, 2002L, "item2", 1620000007L),
      ParquetCombinerRDD.DataA(2L, 203L, 2003L, "item2", 1620000008L),
      ParquetCombinerRDD.DataA(2L, 204L, 2004L, "item2", 1620000009L),
      ParquetCombinerRDD.DataA(2L, 205L, 2005L, "item3", 1620000010L),
      ParquetCombinerRDD.DataA(2L, 206L, 2006L, "item3", 1620000011L)
    ))
    
    val dataB = sc.parallelize(Seq(
      ParquetCombinerRDD.DataB(1L, "Location A"),
      ParquetCombinerRDD.DataB(2L, "Location B")
    ))
    
    // Process the test data
    val topX = 3  // Get top 3 items per location
    val result = ParquetCombinerRDD.processRDDs(dataA, dataB, topX).collect()
    
    // Verify results
    
    // Check total result count: 2 locations x 3 items per location = 6 results
    assert(result.length === 6)
    
    // Verify Location A ranking: item1 (rank 1), item2 (rank 2), item3 (rank 3)
    val locationAResults = result.filter(_.geographical_location == "Location A")
    assert(locationAResults.length === 3)
    assert(locationAResults.exists(r => r.item_name == "item1" && r.item_rank == "1"))
    assert(locationAResults.exists(r => r.item_name == "item2" && r.item_rank == "2"))
    assert(locationAResults.exists(r => r.item_name == "item3" && r.item_rank == "3"))
    
    // Verify Location B ranking: item2 (rank 1), item3 (rank 2), item1 (rank 3)
    val locationBResults = result.filter(_.geographical_location == "Location B")
    assert(locationBResults.length === 3)
    assert(locationBResults.exists(r => r.item_name == "item2" && r.item_rank == "1"))
    assert(locationBResults.exists(r => r.item_name == "item3" && r.item_rank == "2"))
    assert(locationBResults.exists(r => r.item_name == "item1" && r.item_rank == "3"))
  }
  
  test("processRDDs should respect the topX parameter") {
    // Create test data with various item counts
    val dataA = sc.parallelize(Seq(
      // Location has 5 different items
      ParquetCombinerRDD.DataA(1L, 101L, 1001L, "item1", 1620000000L),
      ParquetCombinerRDD.DataA(1L, 102L, 1002L, "item2", 1620000001L),
      ParquetCombinerRDD.DataA(1L, 103L, 1003L, "item3", 1620000002L),
      ParquetCombinerRDD.DataA(1L, 104L, 1004L, "item4", 1620000003L),
      ParquetCombinerRDD.DataA(1L, 105L, 1005L, "item5", 1620000004L)
    ))
    
    val dataB = sc.parallelize(Seq(
      ParquetCombinerRDD.DataB(1L, "Location A")
    ))
    
    // Test with topX = 2
    val topX = 2
    val result = ParquetCombinerRDD.processRDDs(dataA, dataB, topX).collect()
    
    // Verify results
    // Should only return 2 results for Location A
    assert(result.length === 2)
    
    // Check that ranks are correct
    val ranks = result.map(_.item_rank).sorted
    assert(ranks.sameElements(Array("1", "2")))
    
    // Test with topX = 4
    val topX2 = 4
    val result2 = ParquetCombinerRDD.processRDDs(dataA, dataB, topX2).collect()
    
    // Verify results
    // Should return 4 results for Location A
    assert(result2.length === 4)
    
    // Check that ranks are correct
    val ranks2 = result2.map(_.item_rank).sorted
    assert(ranks2.sameElements(Array("1", "2", "3", "4")))
  }
  
  test("processRDDs should handle empty input correctly") {
    // Create empty test data
    val emptyDataA = sc.parallelize(Seq.empty[ParquetCombinerRDD.DataA])
    val dataB = sc.parallelize(Seq(
      ParquetCombinerRDD.DataB(1L, "Location A"),
      ParquetCombinerRDD.DataB(2L, "Location B")
    ))
    
    // Process the test data
    val topX = 3
    val result = ParquetCombinerRDD.processRDDs(emptyDataA, dataB, topX).collect()
    
    // Verify results
    // Should return empty result
    assert(result.isEmpty)
  }
}