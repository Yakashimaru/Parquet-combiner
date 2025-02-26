import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.log4j.{Level, Logger}

object TestApp {
  def main(args: Array[String]): Unit = {
    // Logger.getLogger("org").setLevel(Level.ERROR)
    // Logger.getLogger("akka").setLevel(Level.ERROR)
    Logger.getRootLogger.setLevel(Level.OFF)
    // Initialize Spark session
    val spark = SparkSession.builder
      .appName("Parquet Reader")
      .master("local[*]")  // Adjust depending on your environment
      .getOrCreate()

    // Read the Parquet file
    val readParquet = spark.read.parquet("data/dataA")

    // Show the content of the Parquet file
    readParquet.show()

    // Number of rows
    val rowCount = readParquet.count()
    println(s"Number of rows: $rowCount")

    // Number of columns
    val columnCount = readParquet.columns.length
    println(s"Number of columns: $columnCount")

    // Column names and types
    println("Schema of the dataset:")
    readParquet.printSchema()

    // Summary statistics for numeric columns
    println("Summary statistics for numeric columns:")
    readParquet.describe("geographical_location_oid", "video_camera_oid", "detection_oid", "timestamp_detected").show()

    // Stop the Spark session
    spark.stop()
  }
}
