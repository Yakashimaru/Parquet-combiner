// HTX Data Engineering Assessment
// Created by: Joel John Tan
// Date: March 2025

package com.htx

import org.apache.spark.sql.{SparkSession, Row, SaveMode}
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.storage.StorageLevel
import com.htx.models.Models.{
  TopItemResult,
  ItemCountResult,
  LocationStatsResult,
  DataA,
  DataB
}
import com.htx.services.{AggregationFactory, AggregationOperation}
import com.htx.utils.{DataReader, Logging}
import org.apache.spark.rdd.RDD

object ParquetCombinerRDD extends Logging {
  private val DefaultTopXItems = 5
  private val SampleNoRows = 3
  private val DefaultNoRows = 10

  def main(args: Array[String]): Unit = {
    // Create and manage Spark session
    val spark = SparkSession
      .builder()
      .appName("ParquetCombinerRDD")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "200")
      .config("spark.default.parallelism", "200")
      .config("spark.rdd.compress", "true")
      .getOrCreate()

    try {
      processParquetFiles(spark, parseArgs(args))
    } finally {
      spark.stop()
    }
  }

  private def parseArgs(args: Array[String]): (String, String, String, Int) = {
    val dataAPath = args.lift(0).getOrElse("src/test/resources/test-data/dataA")
    val dataBPath = args.lift(1).getOrElse("src/test/resources/test-data/dataB")
    val outputPath =
      args.lift(2).getOrElse("src/test/resources/test-data/output")
    val topX = args.lift(3).map(_.toInt).getOrElse(DefaultTopXItems)
    (dataAPath, dataBPath, outputPath, topX)
  }

  def processParquetFiles(
      spark: SparkSession,
      paths: (String, String, String, Int)
  ): Unit = {
    val (dataAPath, dataBPath, outputPath, topX) = paths

    try {
      // Log input details
      logger.info(s"""
        |===== PARQUET COMBINER =====
        |Input A: $dataAPath
        |Input B: $dataBPath
        |Output: $outputPath
        |Top X: $topX
      """.stripMargin)

      // Read and cache input RDDs with explicit type casting
      val cachedDataA: RDD[DataA] = DataReader
        .readParquetA(spark, dataAPath)
        .asInstanceOf[RDD[DataA]]
        .persist(StorageLevel.MEMORY_AND_DISK_SER)
      val cachedDataB: RDD[DataB] = DataReader
        .readParquetB(spark, dataBPath)
        .asInstanceOf[RDD[DataB]]
        .persist(StorageLevel.MEMORY_AND_DISK_SER)

      // Log sample data
      logSampleData(cachedDataA, cachedDataB)

      // Perform aggregations
      val params = Map("topX" -> topX)
      val (resultRDD, itemCountRDD, locationStatsRDD) = performAggregations(
        cachedDataA,
        cachedDataB,
        params
      )
      // Write results to Parquet
      writeResultToParquet(spark, resultRDD, cachedDataB, outputPath)

      // Clean up cached RDDs
      cachedDataA.unpersist()
      cachedDataB.unpersist()

      // Final logging
      logAggregationResults(resultRDD, outputPath)
    } catch {
      case e: Exception =>
        logger.error(s"ERROR: ${e.getMessage}")
        e.printStackTrace()
    }
  }

  private def logSampleData(
      dataARDD: RDD[DataA],
      dataBRDD: RDD[DataB]
  ): Unit = {
    logger.info("\nDataA Sample (3 records):")
    dataARDD.take(SampleNoRows).foreach(record => logger.info(record.toString))

    logger.info("\nDataB Sample (3 records):")
    dataBRDD.take(SampleNoRows).foreach(record => logger.info(record.toString))
  }

  private def performAggregations(
      cachedDataA: RDD[DataA],
      cachedDataB: RDD[DataB],
      params: Map[String, Any]
  ): (
      RDD[TopItemResult],
      RDD[ItemCountResult],
      RDD[LocationStatsResult]
  ) = {
    // Top Items Aggregation
    val topItemsAggregation = AggregationFactory.createAggregation("topItems")
    val resultRDD = topItemsAggregation
      .asInstanceOf[AggregationOperation[TopItemResult]]
      .aggregate(cachedDataA, cachedDataB, params)

    // Item Count Aggregation
    val itemCountAggregation = AggregationFactory.createAggregation("itemCount")
    val itemCountRDD = itemCountAggregation
      .asInstanceOf[AggregationOperation[ItemCountResult]]
      .aggregate(cachedDataA, cachedDataB, params)

    // Location Stats Aggregation
    val locationStatsAggregation =
      AggregationFactory.createAggregation("locationStats")
    val locationStatsRDD = locationStatsAggregation
      .asInstanceOf[AggregationOperation[LocationStatsResult]]
      .aggregate(cachedDataA, cachedDataB, params)

    // Log result samples
    logAggregationSamples(resultRDD, itemCountRDD, locationStatsRDD)

    (resultRDD, itemCountRDD, locationStatsRDD)
  }

  private def logAggregationSamples(
      resultRDD: RDD[TopItemResult],
      itemCountRDD: RDD[ItemCountResult],
      locationStatsRDD: RDD[LocationStatsResult]
  ): Unit = {
    logger.info("\nTop Items Result Sample (10 records):")
    resultRDD
      .take(DefaultNoRows)
      .foreach(record => logger.info(record.toString))

    logger.info("\nItem Count Result Sample (5 records):")
    itemCountRDD
      .take(DefaultNoRows)
      .foreach(record => logger.info(record.toString))

    logger.info("\nLocation Stats Result Sample (5 records):")
    locationStatsRDD
      .take(DefaultNoRows)
      .foreach(record => logger.info(record.toString))
  }

  private def writeResultToParquet(
      spark: SparkSession,
      resultRDD: RDD[TopItemResult],
      dataBRDD: RDD[DataB],
      outputPath: String
  ): Unit = {
    // Create a mapping of location ID from DataB
    val locationMap = dataBRDD
      .map(loc => (loc.geographical_location_oid, loc.geographical_location))
      .collectAsMap()

    // Broadcast the mapping to all executors
    val broadcastMap = spark.sparkContext.broadcast(locationMap)

    // Use the mapping to include location names in the results
    val joinedResults = resultRDD.map(r => {
      // Look up the location name from the broadcast map
      val locationName =
        broadcastMap.value.getOrElse(r.geographical_location_oid, "Unknown")
      Row(locationName, r.item_rank, r.item_name)
    })

    // Define output schema
    val outputSchema = StructType(
      Seq(
        StructField("geographical_location", StringType, true),
        StructField("item_rank", StringType, true),
        StructField("item_name", StringType, true)
      )
    )

    // Convert to DataFrame and write
    val resultDF = spark.createDataFrame(joinedResults, outputSchema)
    resultDF.write
      .mode(SaveMode.Overwrite)
      .option("compression", "snappy")
      .parquet(outputPath)
  }

  private def logAggregationResults(
      resultRDD: RDD[TopItemResult],
      outputPath: String
  ): Unit = {
    logger.info(s"\nProcess completed successfully.")
    logger.info(s"Output written to: $outputPath")
    logger.info(s"Result contains ${resultRDD.count()} records")
    logger.info("===== PROCESSING COMPLETE =====\n")
  }
}
