import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._

object Main {
  def main(args: Array[String]): Unit = {
    // Initialize Spark session
    val spark = SparkSession.builder()
      .appName("CreateDatasetA")
      .master("local[*]")
      .getOrCreate()

    // Define the schema for the dataset
    val schema = StructType(Array(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("age", IntegerType, nullable = false),
      StructField("city", StringType, nullable = false)
    ))

    // Create a list of manually written rows
    val data = Seq(
      Row(1, "Alice", 29, "New York"),
      Row(2, "Bob", 34, "Los Angeles"),
      Row(3, "Charlie", 25, "Chicago"),
      Row(4, "David", 42, "Houston"),
      Row(5, "Eve", 30, "Phoenix"),
      Row(6, "Frank", 35, "San Francisco"),
      Row(7, "Grace", 28, "Seattle"),
      Row(8, "Hannah", 40, "Boston"),
      Row(9, "Ivy", 33, "Dallas"),
      Row(10, "Jack", 38, "Austin")
    )

    // Create a DataFrame using the schema and the manually specified data
    val datasetA = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    // Show the resulting DataFrame
    datasetA.show()

     // Write the DataFrame as a Parquet file
    datasetA.write.parquet("datasetA.parquet")

    // Stop Spark session
    spark.stop()
  }
}
