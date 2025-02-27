package com.htx

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, Row, SaveMode}
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

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
  
  // Process RDDs to produce the result
  def processRDDs(dataARDD: RDD[DataA], dataBRDD: RDD[DataB], topX: Int): RDD[Result] = {
    // Step 1: Remove duplicate detection_oid values
    val deduplicatedDataA = dataARDD.keyBy(_.detection_oid)
      .reduceByKey((a, _) => a)  // Keep the first occurrence of each detection_oid
      .values
      .cache() // Cache for performance as we'll reuse this RDD
    
    // Step 2: Join with DataB to get geographical locations
    val locationKeyedDataB = dataBRDD.keyBy(_.geographical_location_oid)
      .cache() // Cache for performance
    
    val joined = deduplicatedDataA.keyBy(_.geographical_location_oid)
      .join(locationKeyedDataB)
      .map { case (_, (dataA, dataB)) => 
        (dataB.geographical_location_oid, dataA.item_name)
      }
      
    // Step 3: Count items by location and name
    val counted = joined.map { case (geographical_location_oid, itemName) => 
      ((geographical_location_oid, itemName), 1)
    }.reduceByKey(_ + _)
      .map { case ((geographical_location_oid, itemName), count) => 
        (geographical_location_oid, (itemName, count))
      }
    
    // Step 4: Group by location, then rank items within each location
    val grouped = counted.groupByKey()
    
    val ranked = grouped.flatMap { case (geographical_location_oid, itemsWithCounts) => 
      // Sort items by count (descending) and take top X
      val topItems = itemsWithCounts.toSeq
        .sortBy(-_._2) // Sort by count descending
        .take(topX)    // Take top X
        .zipWithIndex  // Add index for ranking
      
      // Convert to final result format with the corrected field name
      topItems.map { case ((itemName, _), index) => 
        Result(geographical_location_oid, (index + 1).toString, itemName) // index + 1 for 1-based ranking
      }
    }
    
    ranked
  }
}