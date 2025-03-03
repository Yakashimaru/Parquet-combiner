# HTX Data Engineer Technical Test

## Project Overview
This Scala Spark project analyzes object detection data from video cameras across different geographical locations. The application processes Parquet datasets to identify and rank the most popular items detected by object detection algorithms, with a focus on performance optimization and proper handling of data duplication issues.

## Input Data Model
The application processes two Parquet files:

**Dataset A** (~1 Million Rows):
| Column Name | Type | Description |
|-------------|------|-------------|
| geographical_location_oid | bigint | Unique identifier for geographical location |
| video_camera_oid | bigint | Unique identifier for video camera |
| detection_oid | bigint | Unique identifier for detection event |
| item_name | varchar(5000) | Name of detected item |
| timestamp_detected | bigint | Timestamp of detection |

**Dataset B** (~10,000 Rows):
| Column Name | Type | Description |
|-------------|------|-------------|
| geographical_location_oid | bigint | Unique identifier for geographical location |
| geographical_location | varchar(500) | Geographical location description |

## Output Data Model
| Column Name | Type | Description |
|-------------|------|-------------|
| geographical_location | bigint | Unique identifier for geographical location |
| item_rank | varchar(500) | Rank of item (1 = most popular) |
| item_name | varchar(5000) | Name of detected item |

## Note on Output Format

The output format follows the specification's type requirements, using `geographical_location_oid` as the `geographical_location` column with type `bigint` in the output. While it might seem more intuitive to use the descriptive location names from Dataset B, the requirements explicitly specify:

**Output Column Specs:**
- geographical_location | **bigint** | A unique bigint identifier for the geographical location

This suggests that the output is designed to maintain the ID rather than the descriptive name.
The implementation in `ParquetCombinerRDD.scala` follows this interpretation when defining the output schema:

```scala
val outputSchema = StructType(
  Seq(
    StructField("geographical_location", LongType, true),
    StructField("item_rank", StringType, true),
    StructField("item_name", StringType, true)
  )
)
```

## Key Requirements Implementation

1. **Deduplication**: Each `detection_oid` is counted only once using `reduceByKey` operations.
2. **Flexible Configuration**: The application supports runtime configuration of:
   - Input paths for both Parquet files
   - Output path for results
   - Top X items parameter
3. **RDD-Based Processing**: Core logic implemented using Spark RDD API.
4. **Clean Code Guidelines**: Followed Scala best practices with proper error handling and documentation.
5. **Design Patterns**: Implemented Factory and Strategy patterns for reusable aggregation operations.
6. **Performance Optimization**: Optimized for minimal shuffling and efficient memory usage.
7. **Data Skew Handling**: Custom implementation in `SkewedDataHandler` using data salting technique.

## Project Structure

```
project-root/
│
├── src/
│   ├── main/
│   │   ├── scala/
│   │   │   ├── com/htx/
│   │   │   │   ├── ParquetCombinerRDD.scala     # Main processing application
│   │   │   │   ├── models/                      # Data models
│   │   │   │   ├── services/                    # Aggregation services
│   │   │   │   └── utils/                       # Utility classes
│   │   │   │
│   │   │   └── tools/                           # Utility tools
│   │   │       ├── GenerateParquet.scala        # Test data generation utility
│   │   │       └── ReadParquet.scala            # Parquet file reading and analysis tool
│   │   │
│   │   └── resources/                           # Configuration files
│   │
│   └── test/
│       ├── scala/                               # Test classes
│       └── resources/                           # Test data
│
├── project/                                     # SBT project configuration
├── build.sbt                                    # Project dependencies and build settings
└── scalastyle-config.xml                        # Scalastyle configuration
```

## Design Patterns and Architecture

The application implements several design patterns to enable code reusability and maintainability:

1. **Factory Pattern**: `AggregationFactory` creates different aggregation strategies
2. **Strategy Pattern**: `AggregationOperation` defines a common interface for algorithms
3. **Data Skew Handling**: `SkewedDataHandler` implements custom techniques for skewed data

## Performance Optimizations

1. **Efficient Memory Usage**: Strategic caching and early projection
2. **Shuffle Reduction**: Optimized data transfer between executors
3. **Custom Join Implementation**: Avoided explicit `.join()` operations

_Check `considerations.txt` for more details on design choices and optimizations_

## Prerequisites
- Java 17
- Scala 2.12.18
- Apache Spark 3.5.4
- SBT

## Building and Running

### Build Commands
```bash
# Update dependencies
sbt update

# Clean and compile
sbt clean compile

# Run Scalastyle checks
sbt scalastyle

# Create assembly JAR for Spark Submit
sbt assembly
```

### Running the Application
```bash
# Run with default parameters
sbt run

# Run with custom parameters
sbt "run [dataAPath] [dataBPath] [outputPath] [topX]"


# Run with Spark Submit after assembly
spark-submit \
  --class com.htx.ParquetCombinerRDD \
  target/scala-2.12/htx_data_engineer_test_2.12-0.1.0-SNAPSHOT.jar \
  [dataAPath] [dataBPath] [outputPath] [topX]
```

### Utility Tools
```bash
# Generate test data
sbt "runMain tools.GenerateParquet"

# Key parameters:
#   --data-a-rows N        : Number of rows for DataA (default: 1000)
#   --data-b-rows N        : Number of rows for DataB (default: 10)
#   --duplication-rate R   : Percentage of duplicate records (default: 0.15)
#   --skew-location ID     : Location ID with data skew (default: 1)
#   --skew-factor F        : Intensity of data skew (default: 5.0)

# Read Parquet files
sbt "runMain tools.ReadParquet dataA 20"

# Parameters:
#   <file>                 : 'dataA', 'dataB', 'output', or custom filename
#   [limit]                : Number of rows to display (default: 20)
#   [path]                 : Custom directory path
```

### Testing
```bash
# Run all tests
sbt test

# Run unit tests only
sbt "testOnly com.htx.ParquetCombinerRDDUnitTest"

# Run integration tests only
sbt "testOnly com.htx.ParquetCombinerRDDIntegrationTest"
```

## Testing Approach

### Unit Tests
The unit test suite validates individual components:
- TopItemsAggregation functionality
- ItemCountAggregation accuracy
- LocationStatsAggregation calculations
- SkewedDataHandler implementation
- Edge cases (empty inputs, duplicate handling, etc.)

### Integration Tests
The integration test suite validates end-to-end processing:
- Full pipeline execution
- Different topX configurations
- Duplicate detection_oid handling
- Output format correctness

## Design Considerations

1. **Memory Management**:
   - Strategic caching of intermediate results
   - Explicit control of persistence levels
   - Early projection of required fields

2. **Shuffle Operations**:
   - Minimize data movement through early deduplication
   - Use of reduceByKey instead of groupByKey
   - Balanced partition distribution

3. **Extensibility**:
   - Factory pattern for creating aggregation operations
   - Strategy pattern for implementing different aggregation types
   - Base traits for type safety and reusability