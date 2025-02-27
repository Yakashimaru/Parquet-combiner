import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.log4j.{Level, Logger}

object TestApp {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.OFF)

    val dataPath = if (args.length > 0) args(0) else "dataA"

    // Initialize Spark session
    val spark = SparkSession.builder
      .appName("Parquet Reader")
      .master("local[*]")  // Adjust depending on your environment
      .getOrCreate()

    // Read the Parquet file
    val readParquet = spark.read.parquet("data/" + dataPath)

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
    if (dataPath == "dataA") {
      readParquet.describe("geographical_location_oid", "video_camera_oid", "detection_oid", "timestamp_detected").show()
    } else if (dataPath == "dataB") {
      readParquet.describe("geographical_location_oid","geographical_location").show()
    } 
    else {
      readParquet.describe("geographical_location","item_rank","item_name").show()
    }

    // Stop the Spark session
    spark.stop()
  }
}
