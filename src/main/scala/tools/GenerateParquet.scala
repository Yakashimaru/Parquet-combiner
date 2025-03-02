// HTX Data Engineering Assessment
// Created by: Joel John Tan
// Date: March 2025

package tools

import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.functions.desc
import java.time.Instant
import scala.util.Try
import com.htx.utils.Logging

/** Tool for generating test data for the item detection analysis project. This
  * utility generates test data that mimics production data for testing
  * purposes.
  */
object GenerateParquet extends Logging {
  private val sampleNoRows = 5
  private val defaultNoRows = 20

  // Default configuration values
  private val DEFAULT_OUTPUT_DIR = "src/test/resources/test-data"
  private val DEFAULT_DATA_A_ROWS = 1000
  private val DEFAULT_DATA_B_ROWS = 10
  private val DEFAULT_DUPLICATION_RATE =
    0.15 // 15% of detection_oids will be duplicated
  private val DEFAULT_SKEW_LOCATION = 1L // Location ID that will have data skew
  private val DEFAULT_SKEW_FACTOR =
    5.0 // How much more data the skewed location will have

  private val RANDOM_SEED = 42
  private val CAMERAS_PER_LOCATION = 10
  private val TIMESTAMP_VARIATION = 10
  private val DEFAULT_NUM_ITEMS = 10

  def main(args: Array[String]): Unit = {
    // Parse command line arguments
    val config = parseArgs(args)
    logger.info(s"Generating with configuration: $config")

    // Initialize Spark Session
    val spark = SparkSession
      .builder()
      .appName("Test Parquet Data Generator")
      .master(config.sparkMaster)
      .getOrCreate()

    // scalastyle:off underscore.import import.grouping
    import spark.implicits._
    // scalastyle:on underscore.import import.grouping
    try {
      // Generate DataB (Geographical Locations)
      val locationData = generateLocationData(config.dataBRows)
      val locationRDD = spark.sparkContext.parallelize(locationData)
      val dataBDF =
        locationRDD.toDF("geographical_location_oid", "geographical_location")

      // Generate DataA (Detections)
      val detectionData = generateDetectionData(
        config.dataARows,
        locationData.length,
        config.duplicationRate,
        config.skewLocationId,
        config.skewFactor,
        config.numItems
      )
      val detectionRDD = spark.sparkContext.parallelize(detectionData)
      val dataADF = detectionRDD.toDF(
        "geographical_location_oid",
        "video_camera_oid",
        "detection_oid",
        "item_name",
        "timestamp_detected"
      )

      // Create output directories if they don't exist
      val dataAPath = s"${config.outputDir}/dataA"
      val dataBPath = s"${config.outputDir}/dataB"

      // Write DataA to Parquet
      dataADF.write
        .mode(SaveMode.Overwrite)
        .parquet(dataAPath)

      // Write DataB to Parquet
      dataBDF.write
        .mode(SaveMode.Overwrite)
        .parquet(dataBPath)

      // Display statistics and samples
      logger.info(
        s"Generated ${config.dataARows} records for DataA at $dataAPath"
      )
      logger.info(
        s"Generated ${config.dataBRows} records for DataB at $dataBPath"
      )

      // Show sample data
      logger.info("\nDataA Sample:")
      dataADF.show(sampleNoRows)

      logger.info("DataB Sample:")
      dataBDF.show(sampleNoRows)

      // Display data distribution statistics
      logger.info("\nData Distribution by Location:")
      dataADF
        .groupBy("geographical_location_oid")
        .count()
        .join(dataBDF, "geographical_location_oid")
        .select("geographical_location_oid", "geographical_location", "count")
        .orderBy(desc("count"))
        .show(defaultNoRows)

      // Count distinct detection_oids to show duplicates
      val totalRows = dataADF.count()
      val distinctDetections =
        dataADF.select("detection_oid").distinct().count()
      logger.info(
        s"\nTotal rows: $totalRows, Distinct detection_oids: $distinctDetections"
      )
      logger.info(
        s"Duplication rate: ${(totalRows - distinctDetections) * 100.0 / totalRows}%"
      )

    } finally {
      spark.stop()
    }
  }

  /** Generate location data with city names
    */
  private def generateLocationData(numLocations: Int): Seq[(Long, String)] = {
    val cities = Array(
      "New York City",
      "Los Angeles",
      "Chicago",
      "Houston",
      "Phoenix",
      "Philadelphia",
      "San Antonio",
      "San Diego",
      "Dallas",
      "San Jose",
      "Austin",
      "Jacksonville",
      "Fort Worth",
      "Columbus",
      "Indianapolis",
      "Charlotte",
      "Seattle",
      "Denver",
      "Boston",
      "El Paso",
      "Nashville",
      "Detroit",
      "Portland",
      "Memphis",
      "Louisville"
    )

    // Ensure we don't exceed the number of cities we have
    val actualLocations = Math.min(numLocations, cities.length)

    (1 to actualLocations).map(id => (id.toLong, cities(id - 1)))
  }

  /** Generate detection data with configurable parameters
    */
  private def generateDetectionData(
      numRecords: Int,
      numLocations: Int,
      duplicationRate: Double,
      skewLocationId: Long,
      skewFactor: Double,
      numItems: Int
  ): Seq[(Long, Long, Long, String, Long)] = {
    val random = new scala.util.Random(RANDOM_SEED) // For reproducibility
    val currentTime = Instant.now().getEpochSecond

    // Define possible items that might be detected (expand based on numItems parameter)
    val baseItems = Array(
      "person",
      "car",
      "truck",
      "bicycle",
      "motorcycle",
      "dog",
      "cat",
      "bus",
      "traffic light",
      "backpack",
      "fire hydrant",
      "stop sign",
      "parking meter",
      "bench",
      "bird",
      "boat",
      "skateboard",
      "umbrella",
      "handbag",
      "tie",
      "suitcase",
      "frisbee",
      "skis",
      "snowboard",
      "sports ball",
      "kite",
      "baseball bat",
      "surfboard",
      "bottle",
      "wine glass",
      "cup",
      "fork",
      "knife",
      "spoon",
      "bowl",
      "laptop",
      "cell phone",
      "book",
      "clock",
      "vase"
    )

    // Ensure we don't exceed the number of base items we have
    val actualNumItems = Math.min(numItems, baseItems.length)
    val items = baseItems.take(actualNumItems)

    // Calculate the number of records with duplicated detection_oids
    val numDuplicates = (numRecords * duplicationRate).toInt
    val numUniqueRecords = numRecords - numDuplicates

    // Generate unique detection records
    val baseRecords = (1 to numUniqueRecords).map { i =>
      // Apply skew for the specified location
      val locationProb = random.nextDouble()
      val locationOid = {
        if (locationProb < 0.7 * skewFactor / (1 + skewFactor)) {
          skewLocationId
        } else {
          val nonSkewedLoc = random.nextInt(numLocations) + 1
          if (nonSkewedLoc == skewLocationId) {
            (nonSkewedLoc % numLocations) + 1
          } else {
            nonSkewedLoc
          }
        }
      }.toLong


      val cameraOid =
        (locationOid * CAMERAS_PER_LOCATION) + random.nextInt(
          CAMERAS_PER_LOCATION
        ) + 1 // Cameras are grouped by location
      val detectionOid = i.toLong
      val itemName = items(random.nextInt(items.length))
      val timestamp =
        currentTime - random.nextInt(86400 * 30) // Random time in last 30 days

      (locationOid, cameraOid, detectionOid, itemName, timestamp)
    }

    // Generate duplicates for some detection_oids
    val duplicateRecords = (1 to numDuplicates).map { _ =>
      val sourceRecord = baseRecords(random.nextInt(baseRecords.length))
      // Keep the same detection_oid, but potentially vary other fields slightly
      (
        sourceRecord._1,
        sourceRecord._2,
        sourceRecord._3, // Same detection_oid
        sourceRecord._4, // Same item
        sourceRecord._5 + random.nextInt(
          TIMESTAMP_VARIATION
        ) // Slightly different timestamp
      )
    }

    // Combine and shuffle the records
    random.shuffle(baseRecords ++ duplicateRecords)
  }

  // Case class for configuration parameters
  case class Config(
      outputDir: String = DEFAULT_OUTPUT_DIR,
      dataARows: Int = DEFAULT_DATA_A_ROWS,
      dataBRows: Int = DEFAULT_DATA_B_ROWS,
      duplicationRate: Double = DEFAULT_DUPLICATION_RATE,
      skewLocationId: Long = DEFAULT_SKEW_LOCATION,
      skewFactor: Double = DEFAULT_SKEW_FACTOR,
      numItems: Int = DEFAULT_NUM_ITEMS,
      sparkMaster: String = "local[*]"
  )

  /** Parse command line arguments into a Config object. Supports the following
    * options:
    * --output-dir PATH : Directory to write output files
    * --data-a-rows N : Number of rows to generate for DataA
    * --data-b-rows N : Number of rows to generate for DataB
    * --duplication-rate R: Percentage of records with duplicate detection_oids
    * (0.0-1.0)
    * --skew-location ID : Location ID that will have data skew
    * --skew-factor F : Factor determining how much more data the skewed
    * location will have
    * --num-items N : Number of distinct item types to generate
    * --spark-master URL : Spark master URL
    */
  private def parseArgs(args: Array[String]): Config = {
    // Check for help flag first
    if (args.contains("--help")) {
      printHelp()
      System.exit(0)
    }

    // Process arguments recursively
    processArgList(args.toList, Config())
  }

  // Separate function to process a single argument pair
  private def processArgPair(
      option: String,
      value: String,
      config: Config
  ): Config = {
    option match {
      case "--output-dir" =>
        config.copy(outputDir = value)
      case "--data-a-rows" =>
        config.copy(dataARows = Try(value.toInt).getOrElse(DEFAULT_DATA_A_ROWS))
      case "--data-b-rows" =>
        config.copy(dataBRows = Try(value.toInt).getOrElse(DEFAULT_DATA_B_ROWS))
      case "--duplication-rate" =>
        config.copy(duplicationRate =
          Try(value.toDouble).getOrElse(DEFAULT_DUPLICATION_RATE)
        )
      case "--skew-location" =>
        config.copy(skewLocationId =
          Try(value.toLong).getOrElse(DEFAULT_SKEW_LOCATION)
        )
      case "--skew-factor" =>
        config.copy(skewFactor =
          Try(value.toDouble).getOrElse(DEFAULT_SKEW_FACTOR)
        )
      case "--num-items" =>
        config.copy(numItems = Try(value.toInt).getOrElse(DEFAULT_NUM_ITEMS))
      case "--spark-master" =>
        config.copy(sparkMaster = value)
      case _ =>
        logger.info(s"Unknown option: $option")
        config
    }
  }

  // Process the list of arguments recursively
  private def processArgList(args: List[String], config: Config): Config = {
    args match {
      case Nil =>
        config
      case option :: value :: rest if option.startsWith("--") =>
        // Process this option-value pair and continue with the rest
        val updatedConfig = processArgPair(option, value, config)
        processArgList(rest, updatedConfig)
      case unknown :: rest =>
        logger.info(s"Unknown argument format: $unknown")
        processArgList(rest, config)
    }
  }

  /** Print usage help message
    */
  private def printHelp(): Unit = {
    logger.info(
      """Usage: GenerateParquet [options]
        |
        |Options:
        |  --output-dir PATH     Directory to write output files
        |                        Default: src/test/resources/test-data
        |  --data-a-rows N       Number of rows to generate for DataA
        |                        Default: 1000
        |  --data-b-rows N       Number of rows to generate for DataB
        |                        Default: 10
        |  --duplication-rate R  Percentage of records with duplicate detection_oids (0.0-1.0)
        |                        Default: 0.15
        |  --skew-location ID    Location ID that will have data skew
        |                        Default: 1
        |  --skew-factor F       Factor determining how much more data the skewed location will have
        |                        Default: 5.0
        |  --num-items N         Number of distinct item types to generate
        |                        Default: 10
        |  --spark-master URL    Spark master URL
        |                        Default: local[*]
        |  --help                Show this help message
        |
        |Examples:
        |  Generate 10,000 records with 20% duplication:
        |    GenerateParquet --data-a-rows 10000 --duplication-rate 0.2
        |
        |  Generate data with extreme skew on location 3:
        |    GenerateParquet --skew-location 3 --skew-factor 10.0
        |""".stripMargin
    )
  }
}
