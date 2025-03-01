/*
 * My Scala project for HTX Data Engineer Test.
 * This file contains the main application logic for combining Parquet files.
 */
package com.example

package com.htx

import org.apache.spark.rdd.RDD
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, Row, SaveMode}
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

/**
 * ParquetCombinerRDD - A utility to combine data from two Parquet files,
 * deduplicate detection IDs, and find top items by location.
 * Enhanced to support reusable aggregation operations and handling data skew.
 */
object ParquetCombinerRDD {
  // Case classes for the input data structures
  // Adding @SerialVersionUID to ensure serialization compatibility
  @SerialVersionUID(1L)
  case class DataA(
    geographical_location_oid: Long,
    video_camera_oid: Long,
    detection_oid: Long,
    item_name: String,
    timestamp_detected: Long
  ) extends Serializable

  @SerialVersionUID(1L)
  case class DataB(
    geographical_location_oid: Long,
    geographical_location: String
  ) extends Serializable

  // Base trait for all result types to enable reusability
  /* 
   * All result types inherit from this base trait, ensuring they all have a geographical_location_oid.
   */
  sealed trait ResultBase extends Serializable {
    def geographical_location_oid: Long
  }
  
  // Case class for the main output data structure
  @SerialVersionUID(1L)
  case class TopItemResult(
    geographical_location_oid: Long,
    item_rank: String,
    item_name: String
  ) extends ResultBase
  
  // Additional result types for other aggregations (extensible design)
  @SerialVersionUID(1L)
  case class ItemCountResult(
    geographical_location_oid: Long,
    item_name: String,
    count: Long
  ) extends ResultBase
  
  @SerialVersionUID(1L)
  case class LocationStatsResult(
    geographical_location_oid: Long, 
    total_detections: Long,
    unique_items: Long,
    most_active_camera: Long
  ) extends ResultBase

  // Define a trait for aggregation operations to enable reusability
  /* 
   * This trait defines a contract that all aggregation operations must follow. 
   * The generic type T <: ResultBase ensures type safety while allowing different result types.
   */
  trait AggregationOperation[T <: ResultBase] extends Serializable {
    def aggregate(dataA: RDD[DataA], dataB: RDD[DataB], params: Map[String, Any]): RDD[T]
  }

  // Implementation of the top items aggregation
  class TopItemsAggregation extends AggregationOperation[TopItemResult] {
    override def aggregate(dataA: RDD[DataA], dataB: RDD[DataB], params: Map[String, Any]): RDD[TopItemResult] = {
      val topX = params.getOrElse("topX", 5).asInstanceOf[Int]
      
      // Deduplicate detection_oid values while preserving location_oid and item_name
      val deduplicatedDetections = dataA
        .map(data => (data.detection_oid, (data.geographical_location_oid, data.item_name)))
        .reduceByKey((a, _) => a)
        .values
      
      // Count items by location and name
      val itemCounts = deduplicatedDetections
        .map { case (locationOid, itemName) => ((locationOid, itemName), 1L) }
        .reduceByKey(_ + _)
      
      // Get top X items per location - using repartitionAndSortWithinPartitions for better performance
      val topItemsByLocation = itemCounts
        .map { case ((locationOid, itemName), count) => (locationOid, (itemName, count)) }
        .groupByKey()
        .flatMap { case (locationOid, items) => 
          items.toSeq
            .sortBy(-_._2)
            .take(topX)
            .zipWithIndex
            .map { case ((itemName, _), index) => 
              TopItemResult(locationOid, (index + 1).toString, itemName) 
            }
        }
      
      topItemsByLocation
    }
  }
  
  // Implementation of the item count aggregation
  class ItemCountAggregation extends AggregationOperation[ItemCountResult] {
    override def aggregate(dataA: RDD[DataA], dataB: RDD[DataB], params: Map[String, Any]): RDD[ItemCountResult] = {
      // Deduplicate detection_oid values
      val deduplicatedDetections = dataA
        .map(data => (data.detection_oid, (data.geographical_location_oid, data.item_name)))
        .reduceByKey((a, _) => a)
        .values
      
      // Count items and return as ItemCountResult
      deduplicatedDetections
        .map { case (locationOid, itemName) => ((locationOid, itemName), 1L) }
        .reduceByKey(_ + _)
        .map { case ((locationOid, itemName), count) => ItemCountResult(locationOid, itemName, count) }
    }
  }
  
  // Implementation of location stats aggregation
  class LocationStatsAggregation extends AggregationOperation[LocationStatsResult] {
    override def aggregate(dataA: RDD[DataA], dataB: RDD[DataB], params: Map[String, Any]): RDD[LocationStatsResult] = {
      // Deduplicate detection_oid values but keep full DataA record
      val deduplicatedDetections = dataA
        .map(data => (data.detection_oid, data))
        .reduceByKey((a, _) => a)
        .values
      
      // Calculate statistics per location
      deduplicatedDetections
        .map(data => (data.geographical_location_oid, (data.item_name, data.video_camera_oid)))
        .groupByKey()
        .map { case (locationOid, items) => 
          // Count unique items and most active camera
          val uniqueItems = items.map(_._1).toSet.size
          val cameraToCount = items.groupBy(_._2).mapValues(_.size)
          val mostActiveCamera = if (cameraToCount.isEmpty) -1L else cameraToCount.maxBy(_._2)._1
          
          LocationStatsResult(
            locationOid, 
            items.size,  // total detections
            uniqueItems, // unique items
            mostActiveCamera
          )
        }
    }
  }

  // Factory for creating aggregation operations
  /*  
   * Ability to select different aggregation implementations at runtime using a simple string identifier. 
   */
  object AggregationFactory {
    def createAggregation(name: String): AggregationOperation[_ <: ResultBase] = name match {
      case "topItems" => new TopItemsAggregation()
      case "itemCount" => new ItemCountAggregation()
      case "locationStats" => new LocationStatsAggregation()
      case _ => throw new IllegalArgumentException(s"Unknown aggregation type: $name")
    }
  }

  def main(args: Array[String]): Unit = {
    // Parse command line arguments or use defaults
    val dataAPath = if (args.length > 0) args(0) else "data/dataA"
    val dataBPath = if (args.length > 1) args(1) else "data/dataB"
    val outputPath = if (args.length > 2) args(2) else "data/output"
    val topX = if (args.length > 3) args(3).toInt else 5 // Default to top 5 items
    
    // Create Spark session for standalone execution
    val spark = SparkSession.builder()
      .appName("ParquetCombinerRDD")
      .master("local[*]")
      // Remove the problematic serializer configuration
      .config("spark.sql.shuffle.partitions", "200")
      .config("spark.default.parallelism", "200")
      .config("spark.rdd.compress", "true") // Compress RDDs to save memory
      .getOrCreate()
    
    try {
      runWithSpark(spark, dataAPath, dataBPath, outputPath, topX)
    } finally {
      spark.stop()
    }
  }

  // To allow existing SparkSession to be passed in
  def runWithSpark(spark: SparkSession, dataAPath: String, dataBPath: String, outputPath: String, topX: Int): Unit = {
    // Silence loggers
    Logger.getRootLogger.setLevel(Level.ERROR)
    
    try {
      println("\n===== PARQUET COMBINER =====")
      println(s"Input A: $dataAPath")
      println(s"Input B: $dataBPath")
      println(s"Output: $outputPath")
      println(s"Top X: $topX")
      
      // Read Parquet files into RDDs
      val dataARDD = readParquetA(spark, dataAPath)
      val dataBRDD = readParquetB(spark, dataBPath)
      
      // Show sample data
      println("\nDataA Sample (3 records):")
      dataARDD.take(3).foreach(println)
      
      println("\nDataB Sample (3 records):")
      dataBRDD.take(3).foreach(println)
      
      // Cache the input RDDs for reuse across multiple aggregations
      val cachedDataA = dataARDD.persist(StorageLevel.MEMORY_AND_DISK_SER)
      val cachedDataB = dataBRDD.persist(StorageLevel.MEMORY_AND_DISK_SER)
      
      // Process data using the reusable aggregation framework
      val params = Map("topX" -> topX)
      
      // Execute different aggregations using the same framework
      val topItemsAggregation = AggregationFactory.createAggregation("topItems")
      val resultRDD = topItemsAggregation.asInstanceOf[AggregationOperation[TopItemResult]]
        .aggregate(cachedDataA, cachedDataB, params)
      
      // Example of reusing the framework for a different aggregation
      val itemCountAggregation = AggregationFactory.createAggregation("itemCount")
      val itemCountRDD = itemCountAggregation.asInstanceOf[AggregationOperation[ItemCountResult]]
        .aggregate(cachedDataA, cachedDataB, params)
      
      val locationStatsAggregation = AggregationFactory.createAggregation("locationStats")
      val locationStatsRDD = locationStatsAggregation.asInstanceOf[AggregationOperation[LocationStatsResult]]
        .aggregate(cachedDataA, cachedDataB, params)
      
      // Show result samples
      println("\nTop Items Result Sample (10 records):")
      resultRDD.take(10).foreach(println)
      
      println("\nItem Count Result Sample (5 records):")
      itemCountRDD.take(5).foreach(println)
      
      println("\nLocation Stats Result Sample (5 records):")
      locationStatsRDD.take(5).foreach(println)
      
      // Define output schema explicitly for the top items
      val outputSchema = StructType(Seq(
        StructField("geographical_location", LongType, true), 
        StructField("item_rank", StringType, true),
        StructField("item_name", StringType, true)
      ))
      
      // Convert to DataFrame for writing to Parquet
      val resultDF = spark.createDataFrame(
        resultRDD.map(r => Row(r.geographical_location_oid, r.item_rank, r.item_name)),
        outputSchema
      )
      
      // Write result to Parquet
      resultDF.write
        .mode(SaveMode.Overwrite)
        .option("compression", "snappy") // Better compression
        .parquet(outputPath)
      
      // Clean up by unpersisting cached RDDs
      cachedDataA.unpersist()
      cachedDataB.unpersist()
      
      println(s"\nProcess completed successfully.")
      println(s"Output written to: $outputPath")
      println(s"Result contains ${resultRDD.count()} records")
      println("===== PROCESSING COMPLETE =====\n")
    } catch {
      case e: Exception =>
        println(s"ERROR: ${e.getMessage}")
        e.printStackTrace()
    } 
  }
  
  // Read DataA from Parquet file
  def readParquetA(spark: SparkSession, path: String): RDD[DataA] = {
    val df = spark.read.parquet(path)
    df.rdd.map(row => 
      DataA(
        row.getAs[Long]("geographical_location_oid"),
        row.getAs[Long]("video_camera_oid"),
        row.getAs[Long]("detection_oid"),
        row.getAs[String]("item_name"),
        row.getAs[Long]("timestamp_detected")
      )
    )
  }
  
  // Read DataB from Parquet file
  def readParquetB(spark: SparkSession, path: String): RDD[DataB] = {
    val df = spark.read.parquet(path)
    df.rdd.map(row => 
      DataB(
        row.getAs[Long]("geographical_location_oid"),
        row.getAs[String]("geographical_location")
      )
    )
  }
  
  /**
   * Implementation for handling data skew in specific geographical locations
   * Custom implementation of join to avoid using Spark's join operation.
   * Uses salting technique to better distribute the data
   */
  def processSkewedData(dataARDD: RDD[DataA], skewedLocationId: Long, numPartitions: Int): RDD[(Long, String, Int)] = {
    // Filter out records for the skewed location
    val skewedData = dataARDD.filter(_.geographical_location_oid == skewedLocationId)
    val normalData = dataARDD.filter(_.geographical_location_oid != skewedLocationId)
    
    // Process normal data normally
    val normalResults = normalData
      .map(data => ((data.geographical_location_oid, data.detection_oid), data.item_name))
      .reduceByKey((a, _) => a)  // Deduplicate by detection_oid
      .map { case ((locId, _), itemName) => ((locId, itemName), 1) }
      .reduceByKey(_ + _)
      .map { case ((locId, itemName), count) => (locId, itemName, count) }
    
    // Process skewed data with salting technique
    val saltedSkewedResults = skewedData
      .map(data => {
        // Add a salt value (0 to numPartitions-1) to create better distribution
        val salt = (data.detection_oid % numPartitions).toInt
        ((skewedLocationId, data.detection_oid, salt), data.item_name)
      })
      .reduceByKey((a, _) => a)  // Deduplicate by detection_oid with salt
      .map { case ((locId, _, _), itemName) => ((locId, itemName), 1) }
      .reduceByKey(_ + _)
      .map { case ((locId, itemName), count) => (locId, itemName, count) }
    
    // Combine results
    normalResults.union(saltedSkewedResults)
  }
}