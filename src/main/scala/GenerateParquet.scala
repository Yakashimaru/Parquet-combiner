import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.sql.Timestamp
import java.time.Instant

object GenerateParquet {
  def main(args: Array[String]): Unit = {
    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("Dummy Parquet Data Generator")
      .master("local[*]")
      .getOrCreate()
    
    import spark.implicits._
    
    // Define schemas
    val dataASchema = StructType(Seq(
      StructField("geographical_location_oid", LongType, nullable = false),
      StructField("video_camera_oid", LongType, nullable = false),
      StructField("detection_oid", LongType, nullable = false),
      StructField("item_name", StringType, nullable = false),
      StructField("timestamp_detected", LongType, nullable = false)
    ))
    
    val dataBSchema = StructType(Seq(
      StructField("geographical_location_oid", LongType, nullable = false),
      StructField("geographical_location", StringType, nullable = false)
    ))
    
    // Define sample data for DataB (Geographical Locations)
    val locationData = Seq(
      (1L, "New York City"),
      (2L, "Los Angeles"),
      (3L, "Chicago"),
      (4L, "Houston"),
      (5L, "Phoenix"),
      (6L, "Philadelphia"),
      (7L, "San Antonio"),
      (8L, "San Diego"),
      (9L, "Dallas"),
      (10L, "San Jose")
    )
    
    // Generate DataB using RDD
    val locationRDD = spark.sparkContext.parallelize(locationData)
    val dataBDF = locationRDD.toDF("geographical_location_oid", "geographical_location")
    
    // Generate sample data for DataA (Detections)
    // Items that might be detected by video cameras
    val items = Array("person", "car", "truck", "bicycle", "motorcycle", "dog", "cat", "bus", "traffic light", "backpack")
    
    // Function to generate random detection data
    def generateDetectionData(numRecords: Int): Seq[(Long, Long, Long, String, Long)] = {
      val random = new scala.util.Random(42) // For reproducibility
      val currentTime = Instant.now().getEpochSecond
      
      (1 to numRecords).map { i =>
        val locationOid = random.nextInt(10) + 1
        val cameraOid = random.nextInt(100) + 1
        val detectionOid = i.toLong
        val itemName = items(random.nextInt(items.length))
        val timestamp = currentTime - random.nextInt(86400 * 30) // Random time in last 30 days
        
        (locationOid.toLong, cameraOid.toLong, detectionOid, itemName, timestamp)
      }
    }
    
    // Generate DataA using RDD
    val detectionData = generateDetectionData(1000) // Generate 1000 detection records
    val detectionRDD = spark.sparkContext.parallelize(detectionData)
    val dataADF = detectionRDD.toDF("geographical_location_oid", "video_camera_oid", "detection_oid", "item_name", "timestamp_detected")
    
    // Write DataA to Parquet
    dataADF.write
      .mode(SaveMode.Overwrite)
      .parquet("data/dataA")
      
    // Write DataB to Parquet
    dataBDF.write
      .mode(SaveMode.Overwrite)
      .parquet("data/dataB")
      
    // Show sample data
    println("DataA Sample:")
    dataADF.show(5)
    
    println("DataB Sample:")
    dataBDF.show(5)
    
    // Optional: Verify the data
    println("Verifying written data...")
    val readDataA = spark.read.parquet("data/dataA")
    val readDataB = spark.read.parquet("data/dataB")
    
    println(s"DataA count: ${readDataA.count()}")
    println(s"DataB count: ${readDataB.count()}")
    
    spark.stop()
  }
}