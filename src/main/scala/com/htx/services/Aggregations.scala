// HTX Data Engineering Assessment
// Created by: Joel John Tan
// Date: March 2025

package com.htx.services

import org.apache.spark.rdd.RDD
import com.htx.models.Models.{
  DataA,
  DataB,
  TopItemResult,
  ItemCountResult,
  LocationStatsResult
}

/** Implementations of different aggregation strategies.
  *
  * This object contains concrete implementations of the AggregationOperation
  * trait, each following a similar pattern but producing different geographical
  * aggregations.
  *
  * Code reusability is achieved through:
  *   1. Common pattern for handling duplicate detection_oid values 2.
  *      Consistent approach to grouping by geographical_location_oid 3. Shared
  *      transformation patterns with specialized aggregation logic
  */

object Aggregations {
  private val DefaultTopItems = 5

  // Implementation of the top items aggregation
  class TopItemsAggregation extends AggregationOperation[TopItemResult] {
    override def aggregate(
        dataA: RDD[DataA],
        dataB: RDD[DataB],
        params: Map[String, Any]
    ): RDD[TopItemResult] = {
      val topX = params.getOrElse("topX", DefaultTopItems).asInstanceOf[Int]

      // Deduplicate detection_oid values while preserving location_oid and item_name
      val deduplicatedDetections = dataA
        .map(data =>
          (data.detection_oid, (data.geographical_location_oid, data.item_name))
        )
        .reduceByKey((a, _) => a)
        .values

      // Count items by location and name
      val itemCounts = deduplicatedDetections
        .map { case (locationOid, itemName) => ((locationOid, itemName), 1L) }
        .reduceByKey(_ + _)

      // Get top X items per location - using repartitionAndSortWithinPartitions for better performance
      val topItemsByLocation = itemCounts
        .map { case ((locationOid, itemName), count) =>
          (locationOid, (itemName, count))
        }
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
    override def aggregate(
        dataA: RDD[DataA],
        dataB: RDD[DataB],
        params: Map[String, Any]
    ): RDD[ItemCountResult] = {
      // Deduplicate detection_oid values
      val deduplicatedDetections = dataA
        .map(data =>
          (data.detection_oid, (data.geographical_location_oid, data.item_name))
        )
        .reduceByKey((a, _) => a)
        .values

      // Count items and return as ItemCountResult
      deduplicatedDetections
        .map { case (locationOid, itemName) => ((locationOid, itemName), 1L) }
        .reduceByKey(_ + _)
        .map { case ((locationOid, itemName), count) =>
          ItemCountResult(locationOid, itemName, count)
        }
    }
  }

  // Implementation of location stats aggregation
  class LocationStatsAggregation
      extends AggregationOperation[LocationStatsResult] {
    override def aggregate(
        dataA: RDD[DataA],
        dataB: RDD[DataB],
        params: Map[String, Any]
    ): RDD[LocationStatsResult] = {
      // Deduplicate detection_oid values but keep full DataA record
      val deduplicatedDetections = dataA
        .map(data => (data.detection_oid, data))
        .reduceByKey((a, _) => a)
        .values

      // Calculate statistics per location
      deduplicatedDetections
        .map(data =>
          (
            data.geographical_location_oid,
            (data.item_name, data.video_camera_oid)
          )
        )
        .groupByKey()
        .map { case (locationOid, items) =>
          // Count unique items and most active camera
          val uniqueItems = items.map(_._1).toSet.size
          val cameraToCount =
            items.groupBy(_._2).map { case (k, v) => (k, v.size) }.toMap
          val mostActiveCamera =
            if (cameraToCount.isEmpty) -1L else cameraToCount.maxBy(_._2)._1

          LocationStatsResult(
            locationOid,
            items.size, // total detections
            uniqueItems, // unique items
            mostActiveCamera
          )
        }
    }
  }
}
