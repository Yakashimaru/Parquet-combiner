package com.htx.util

import org.apache.spark.sql.{SparkSession, SaveMode}
import scala.util.Random
import java.io.File

/**
 * Utility to generate test data for ParquetCombinerRDD
 * This can be used to create larger test datasets than the ones in the integration tests
 */
object TestDataGenerator {
  
  def main(args: Array[String]): Unit = {
    // Parse command line arguments
    val numLocations = if (args.length > 0) args(0).toInt else 10
    val numItemsPerLocation = if (args.length > 1) args(1).toInt else 1000
    val duplicatePercentage = if (args.length > 2) args(2).toInt else 10
    val outputDirA = if (args.length > 3) args(3) else "test-data/dataA"
    val outputDirB = if (args.length > 4) args(4) else "test-data/dataB"
    
    // Create Spark session
    val spark = SparkSession.builder()
      .appName("TestDataGenerator")
      .master("local[*]")
      .getOrCreate()
    
    import spark.implicits._
    
    try {
      // Ensure output directories exist
      new File(outputDirA).mkdirs()
      new File(outputDirB).mkdirs()
      
      // Generate DataA
      println(s"Generating DataA with $numLocations locations and $numItemsPerLocation items per location...")
      
      val random = new Random(42)  // Use fixed seed for reproducibility
      val itemNames = Seq("apple", "banana", "orange", "grape", "watermelon", 
                         "pear", "strawberry", "blueberry", "mango", "peach",
                         "pineapple", "kiwi", "lemon", "lime", "plum",
                         "cherry", "avocado", "coconut", "fig", "guava")
      
      // Generate locations
      val locations = (1 to numLocations).map(_.toLong).toArray
      
      // Generate DataA rows
      var detectionId = 1000L
      val dataARows = locations.flatMap { locationId =>
        val locationItems = (1 to numItemsPerLocation).map { _ =>
          val cameraId = random.nextInt(100) + 100L
          val itemName = itemNames(random.nextInt(itemNames.length))
          val timestamp = 1620000000L + random.nextInt(1000000)
          
          // For some percentage of items, reuse the previous detection_oid to simulate duplicates
          val useExistingId = random.nextInt(100) < duplicatePercentage && detectionId > 1000L
          val detectionOid = if (useExistingId) detectionId - 1 else { detectionId += 1; detectionId }
          
          (locationId, cameraId, detectionOid, itemName, timestamp)
        }
        locationItems
      }
      
      // Create and write DataA
      val dataADF = dataARows.toSeq.toDF(
        "geographical_location_oid", 
        "video_camera_oid", 
        "detection_oid", 
        "item_name", 
        "timestamp_detected"
      )
      
      dataADF.write.mode(SaveMode.Overwrite).parquet(outputDirA)
      println(s"Generated ${dataARows.length} rows for DataA")
      
      // Generate DataB (locations)
      val dataBRows = locations.map { locationId =>
        val locationName = s"Location-${locationId}"
        (locationId, locationName)
      }
      
      // Create and write DataB
      val dataBDF = dataBRows.toSeq.toDF(
        "geographical_location_oid", 
        "geographical_location"
      )
      
      dataBDF.write.mode(SaveMode.Overwrite).parquet(outputDirB)
      println(s"Generated ${dataBRows.length} rows for DataB")
      
      println("Test data generation complete!")
      
    } finally {
      spark.stop()
    }
  }
}