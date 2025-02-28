package com.htx

import org.apache.spark.rdd.RDD
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, Row, SaveMode}
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import scala.collection.Map

/**
 * ParquetCombinerRDD - A utility to combine data from two Parquet files,
 * deduplicate detection IDs, and find top items by location.
 */
object ParquetCombinerRDD {
  
  // Case classes for the input data structures
  case class DataA(
    geographical_location_oid: Long, //bigint
    video_camera_oid: Long, //bigint
    detection_oid: Long, //bigint
    item_name: String, //varchar
    timestamp_detected: Long //bigint
  )
  
  case class DataB(
    geographical_location_oid: Long, //bigint
    geographical_location: String //varchar
  )
  
  // Case class for the output data structure
  case class Result(
    geographical_location: Long, //bigint
    item_rank: String, //varchar
    item_name: String //varchar
  )

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
      // Configure with safe serialization settings
      .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer") // Use Java serializer instead of Kryo for compatibility
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
      
      // Process data using RDD operations
      val resultRDD = processRDDs(dataARDD, dataBRDD, topX)
      
      // Show result sample
      println("\nResult Sample (10 records):")
      resultRDD.take(10).foreach(println)
      
      // Define output schema explicitly
      val outputSchema = StructType(Seq(
        StructField("geographical_location", LongType, true), 
        StructField("item_rank", StringType, true),
        StructField("item_name", StringType, true)
      ))
      
      // Convert to DataFrame for writing to Parquet
      val resultDF = spark.createDataFrame(
        resultRDD.map(r => Row(r.geographical_location, r.item_rank, r.item_name)),
        outputSchema
      )
      
      // Write result to Parquet
      resultDF.write
        .mode(SaveMode.Overwrite)
        .option("compression", "snappy") // Better compression
        .parquet(outputPath)
      
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
   * Custom implementation of join to avoid using Spark's join operation.
   * This reduces shuffle operations by broadcasting the smaller RDD.
   */
  def customJoin[K, V1, V2](left: RDD[(K, V1)], right: RDD[(K, V2)]): RDD[(K, (V1, V2))] = {
    // Collect right RDD to driver as an array of (K, V2) pairs
    val rightArray = right.collect()
    
    // Create a map from the array for lookups
    val rightMap = rightArray.map(pair => (pair._1, pair._2)).toMap
    
    // Broadcast the map to all executors
    val broadcastMap = left.sparkContext.broadcast(rightMap)
    
    // Use mapPartitions instead of flatMap for better performance
    left.mapPartitions(iter => {
      val bMap = broadcastMap.value
      iter.flatMap { case (k, v1) =>
        bMap.get(k) match {
          case Some(v2) => Iterator.single((k, (v1, v2)))
          case None => Iterator.empty
        }
      }
    }, preservesPartitioning = true) // Preserve partitioning information
  }
  
  // Process RDDs to produce the result
  def processRDDs(dataARDD: RDD[DataA], dataBRDD: RDD[DataB], topX: Int): RDD[Result] = {
    // Step 1: Remove duplicate detection_oid values
    // Extract detection_oid as a key
    val keyedByDetectionOid = dataARDD.map(dataA => (dataA.detection_oid, dataA))
    // Reduce by key to keep only the first occurrence
    val deduplicatedDetections = keyedByDetectionOid.reduceByKey((a, _) => a)
    // Extract just the values
    val deduplicatedDataA = deduplicatedDetections.values
    
    // Cache the deduplicated RDD for reuse
    val cachedDeduplicatedDataA = deduplicatedDataA.persist(StorageLevel.MEMORY_AND_DISK_SER)
    
    // Step 2: Prepare for custom join by extracting join keys
    val locationKeyedDataA = cachedDeduplicatedDataA.map(dataA => 
      (dataA.geographical_location_oid, dataA))
    
    val locationKeyedDataB = dataBRDD.map(dataB => 
      (dataB.geographical_location_oid, dataB))
    
    // Cache the smaller dataset (DataB) for reuse
    val cachedLocationKeyedDataB = locationKeyedDataB.persist(StorageLevel.MEMORY_AND_DISK_SER)
    
    // Step 3: Use custom join instead of Spark's join
    val joined = customJoin(locationKeyedDataA, cachedLocationKeyedDataB)
    
    // Step 4: Extract the item name from joined data
    val locationAndItemName = joined.map { case (_, (dataA, dataB)) =>
      (dataB.geographical_location_oid, dataA.item_name)
    }
    
    // Step 5: Count items by location and name
    val locationItemPairs = locationAndItemName.map { 
      case (locationOid, itemName) => ((locationOid, itemName), 1) 
    }
    
    val itemCounts = locationItemPairs.reduceByKey(_ + _)
    
    val locationItemCounts = itemCounts.map { 
      case ((locationOid, itemName), count) => (locationOid, (itemName, count)) 
    }
    
    // Step 6: Group by location and rank items
    val groupedByLocation = locationItemCounts.groupByKey()
    
    val ranked = groupedByLocation.flatMap { case (locationOid, itemsWithCounts) =>
      // Convert iterable to seq for sorting
      val itemsSeq = itemsWithCounts.toSeq
      
      // Sort by count descending
      val sortedItems = itemsSeq.sortBy(_._2)(Ordering[Int].reverse)
      
      // Take top X items
      val topItems = sortedItems.take(topX)
      
      // Create result objects with rank
      topItems.zipWithIndex.map { case ((itemName, _), index) =>
        Result(locationOid, (index + 1).toString, itemName)
      }
    }
    
    // Clean up by unpersisting cached RDDs
    cachedDeduplicatedDataA.unpersist()
    cachedLocationKeyedDataB.unpersist()
    
    ranked
  }
}