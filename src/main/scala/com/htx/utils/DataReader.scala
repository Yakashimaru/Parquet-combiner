// HTX Data Engineering Assessment
// Created by: Joel John Tan
// Date: March 2025

package com.htx.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import com.htx.models.Models._

/** Utility for reading data from Parquet files */
object DataReader {
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
}
