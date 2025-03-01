// HTX Data Engineering Assessment
// Created by: Joel John Tan
// Date: March 2025

package com.htx.utils

import org.apache.spark.rdd.RDD
import com.htx.models.Models._

/** Handler for skewed data processing */
object SkewedDataHandler {

  /** Implementation for handling data skew in specific geographical locations
    * Custom implementation of join to avoid using Spark's join operation. Uses
    * salting technique to better distribute the data
    *
    * @param dataARDD
    *   The input RDD containing all detection data
    * @param skewedLocationId
    *   The ID of the geographical location exhibiting skew
    * @param numPartitions
    *   The number of partitions to distribute skewed data across
    * @return
    *   RDD containing processed data with counts by location and item
    */
  def processSkewedData(
      dataARDD: RDD[DataA],
      skewedLocationId: Long,
      numPartitions: Int
  ): RDD[(Long, String, Int)] = {
    // Split the data into skewed and normal paths for specialized processing
    val skewedData =
      dataARDD.filter(_.geographical_location_oid == skewedLocationId)
    val normalData =
      dataARDD.filter(_.geographical_location_oid != skewedLocationId)

    // Process normal data using the standard approach
    val normalResults = normalData
      .map(data =>
        ((data.geographical_location_oid, data.detection_oid), data.item_name)
      )
      .reduceByKey((a, _) => a) // Deduplicate by detection_oid
      .map { case ((locId, _), itemName) => ((locId, itemName), 1) }
      .reduceByKey(_ + _)
      .map { case ((locId, itemName), count) => (locId, itemName, count) }

    // OPTIMIZATION: Process skewed data using salting technique
    // This distributes the skewed location data across multiple partitions,
    // preventing the bottleneck that would occur with standard processing
    val saltedSkewedResults = skewedData
      .map(data => {
        // Add a salt value based on detection_oid to ensure better distribution
        // This creates multiple keys for the same location, spreading the data
        // across numPartitions different partitions
        val salt = (data.detection_oid % numPartitions).toInt
        ((skewedLocationId, data.detection_oid, salt), data.item_name)
      })
      .reduceByKey((a, _) => a) // Deduplicate by detection_oid with salt
      .map { case ((locId, _, _), itemName) => ((locId, itemName), 1) }
      .reduceByKey(_ + _) // Aggregate counts by location and item
      .map { case ((locId, itemName), count) => (locId, itemName, count) }

    // Combine results from both processing paths
    normalResults.union(saltedSkewedResults)
  }
}
