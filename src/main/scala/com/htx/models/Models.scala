// HTX Data Engineering Assessment
// Created by: Joel John Tan
// Date: March 2025

package com.htx.models

/** Data models for the application */
object Models {
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
  // All result types inherit from this base trait, ensuring they all have a geographical_location_oid.
  // Snake case used to match database column names
  // scalastyle:off method.name
  sealed trait ResultBase extends Serializable {
    def geographical_location_oid: Long
  }
  // scalastyle:on method.name

  // Case class for the main output data structure
  @SerialVersionUID(1L)
  case class TopItemResult(
      geographical_location_oid: Long,
      item_rank: String,
      item_name: String
  ) extends ResultBase

  // Additional result types for other aggregations
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
}
