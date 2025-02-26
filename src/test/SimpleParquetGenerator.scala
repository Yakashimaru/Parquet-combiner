// build.sbt additions:
// libraryDependencies += "com.github.mjakubowski84" %% "parquet4s-core" % "2.8.0"

import java.nio.file.{Files, Paths}
import scala.util.Random
import java.time.Instant

// First, create a case class representing our data structure
case class DetectionRecord(
  geographical_location_oid: Long,
  video_camera_oid: Long,
  detection_oid: Long,
  item_name: String,
  timestamp_detected: Long
)

object SimpleParquetGenerator {
  def main(args: Array[String]): Unit = {
    println("Starting simple parquet generator...")
    
    try {
      // Create output directory
      val outputDir = Paths.get("data")
      if (!Files.exists(outputDir)) {
        Files.createDirectories(outputDir)
        println(s"Created directory: ${outputDir.toAbsolutePath}")
      }
      
      val outputFile = outputDir.resolve("synthetic_datasetA.parquet")
      println(s"Will write to: ${outputFile.toAbsolutePath}")
      
      // Generate 100 sample records
      println("Generating 100 records...")
      val random = new Random()
      val records = (1 to 100).map { i =>
        // Location IDs from 1-20
        val geographical_location_oid = random.nextInt(20) + 1
        
        // Camera IDs from 1-10
        val video_camera_oid = random.nextInt(10) + 1
        
        // Detection ID = sequential number
        val detection_oid = i
        
        // Item names
        val items = Array("Person", "Car", "Bicycle", "Motorcycle", "Truck")
        val item_name = items(random.nextInt(items.length))
        
        // Recent timestamp (within last day)
        val oneDayInMillis = 24 * 60 * 60 * 1000
        val timestamp_detected = System.currentTimeMillis() - random.nextInt(oneDayInMillis)
        
        DetectionRecord(
          geographical_location_oid, 
          video_camera_oid, 
          detection_oid, 
          item_name, 
          timestamp_detected
        )
      }
      
      // Save records as CSV as a fallback option since it's simpler
      println("Writing records to CSV as well...")
      val csvPath = outputDir.resolve("synthetic_datasetA.csv")
      val csvHeader = "geographical_location_oid,video_camera_oid,detection_oid,item_name,timestamp_detected"
      val csvLines = records.map { r => 
        s"${r.geographical_location_oid},${r.video_camera_oid},${r.detection_oid},${r.item_name},${r.timestamp_detected}"
      }
      Files.write(csvPath, (csvHeader +: csvLines).asJava)
      println(s"CSV file written to: ${csvPath.toAbsolutePath}")
      
      // Using parquet4s
      import com.github.mjakubowski84.parquet4s._
      
      println("Writing records to parquet...")
      ParquetWriter.writeAndClose(outputFile.toString, records)
      
      println(s"Parquet file created at: ${outputFile.toAbsolutePath}")
      println("Done!")
    } catch {
      case e: Exception =>
        println(s"Error: ${e.getMessage}")
        e.printStackTrace()
    }
  }
  
  // Helper to convert Scala collection to Java collection
  implicit class CollectionConverter[T](val collection: Seq[T]) {
    def asJava: java.util.List[T] = {
      import scala.jdk.CollectionConverters._
      collection.asJava
    }
  }
}