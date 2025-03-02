// HTX Data Engineering Assessment
// Created by: Joel John Tan
// Date: March 2025

package com.htx

import org.apache.spark.sql.{SparkSession, Row, SaveMode}
// Import classes
import org.apache.spark.sql.types.{
  StructType,
  StructField,
  LongType,
  StringType
}
import org.apache.spark.storage.StorageLevel

// Import classes
import com.htx.models.Models.{
  TopItemResult,
  ItemCountResult,
  LocationStatsResult
}

import com.htx.services.{AggregationFactory, AggregationOperation}

import com.htx.utils.{DataReader, Logging}

/** ParquetCombinerRDD - A utility to combine data from two Parquet files,
  * deduplicate detection IDs, and find top items by location. Enhanced to
  * support reusable aggregation operations and handling data skew.
  */
object ParquetCombinerRDD extends Logging {
  private val DefaultTopXItems = 5
  private val sampleNoRows = 3
  private val defaultNoRows = 10

  // Define a trait for aggregation operations to enable reusability
  def main(args: Array[String]): Unit = {
    // Parse command line arguments or use defaults
    val dataAPath =
      if (args.length > 0) args(0) else "src/test/resources/test-data/dataA"
    val dataBPath =
      if (args.length > 1) args(1) else "src/test/resources/test-data/dataB"
    val outputPath =
      if (args.length > 2) args(2) else "src/test/resources/test-data/output"
    val topX =
      if (args.length > 3) args(3).toInt else DefaultTopXItems

    // Create Spark session for standalone execution
    val spark = SparkSession
      .builder()
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
  def runWithSpark(
      spark: SparkSession,
      dataAPath: String,
      dataBPath: String,
      outputPath: String,
      topX: Int
  ): Unit = {

    try {
      logger.info("\n===== PARQUET COMBINER =====")
      logger.info(s"Input A: $dataAPath")
      logger.info(s"Input B: $dataBPath")
      logger.info(s"Output: $outputPath")
      logger.info(s"Top X: $topX")

      // Read Parquet files into RDDs
      val dataARDD = DataReader.readParquetA(spark, dataAPath)
      val dataBRDD = DataReader.readParquetB(spark, dataBPath)

      // Log sample data
      logger.info("\nDataA Sample (3 records):")
      dataARDD
        .take(sampleNoRows)
        .foreach(record => logger.info(record.toString))

      logger.info("\nDataB Sample (3 records):")
      dataBRDD
        .take(sampleNoRows)
        .foreach(record => logger.info(record.toString))

      // Cache the input RDDs for reuse across multiple aggregations
      val cachedDataA = dataARDD.persist(StorageLevel.MEMORY_AND_DISK_SER)
      val cachedDataB = dataBRDD.persist(StorageLevel.MEMORY_AND_DISK_SER)

      // Process data using the reusable aggregation framework
      val params = Map("topX" -> topX)

      // Execute different aggregations using the same framework
      val topItemsAggregation = AggregationFactory.createAggregation("topItems")
      val resultRDD = topItemsAggregation
        .asInstanceOf[AggregationOperation[TopItemResult]]
        .aggregate(cachedDataA, cachedDataB, params)

      // Example of reusing the framework for a different aggregation
      val itemCountAggregation =
        AggregationFactory.createAggregation("itemCount")
      val itemCountRDD = itemCountAggregation
        .asInstanceOf[AggregationOperation[ItemCountResult]]
        .aggregate(cachedDataA, cachedDataB, params)

      val locationStatsAggregation =
        AggregationFactory.createAggregation("locationStats")
      val locationStatsRDD = locationStatsAggregation
        .asInstanceOf[AggregationOperation[LocationStatsResult]]
        .aggregate(cachedDataA, cachedDataB, params)

      // Show result samples
      logger.info("\nTop Items Result Sample (10 records):")
      resultRDD
        .take(defaultNoRows)
        .foreach(record => logger.info(record.toString))

      logger.info("\nItem Count Result Sample (5 records):")
      itemCountRDD
        .take(defaultNoRows)
        .foreach(record => logger.info(record.toString))

      logger.info("\nLocation Stats Result Sample (5 records):")
      locationStatsRDD
        .take(defaultNoRows)
        .foreach(record => logger.info(record.toString))

      // Define output schema explicitly for the top items
      val outputSchema = StructType(
        Seq(
          StructField("geographical_location", LongType, true),
          StructField("item_rank", StringType, true),
          StructField("item_name", StringType, true)
        )
      )

      // Convert to DataFrame for writing to Parquet
      val resultDF = spark.createDataFrame(
        resultRDD.map(r =>
          Row(r.geographical_location_oid, r.item_rank, r.item_name)
        ),
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

      logger.info(s"\nProcess completed successfully.")
      logger.info(s"Output written to: $outputPath")
      logger.info(s"Result contains ${resultRDD.count()} records")
      logger.info("===== PROCESSING COMPLETE =====\n")
    } catch {
      case e: Exception =>
        logger.error(s"ERROR: ${e.getMessage}")
        e.printStackTrace()
    }
  }
}
