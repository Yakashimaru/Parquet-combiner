# Testing Instructions for ParquetCombinerRDD
This document provides instructions for running the unit and integration tests for the ParquetCombinerRDD application.
## Prerequisites
- Scala 2.12
- SBT 1.0+
- Java 8+
- Apache Spark 3.x

## Project Structure
com.htx/
├── ParquetCombinerRDD.scala           # Main application code
├── ParquetCombinerRDDUnitTest.scala   # Unit tests
├── ParquetCombinerRDDIntegrationTest.scala # Integration tests
└── util/
    └── TestDataGenerator.scala        # Utility to generate test data

## Running the Tests
### Unit Tests
The unit tests focus on testing individual components of the application, particularly the processRDDs method which handles the core business logic.
```bash
sbt "testOnly com.htx.ParquetCombinerRDDUnitTest"
```
These tests verify:
- Deduplication of detection_oid values
- Correct ranking of items by count
- Respecting the topX parameter
- Handling of empty input data

### Integration Tests
The integration tests verify the complete pipeline of the application, including reading Parquet files, processing the data, and writing the output.
```bash
sbt "testOnly com.htx.ParquetCombinerRDDIntegrationTest"
```
These tests verify:
- End-to-end execution with command line arguments
- Output schema correctness
- Behavior with different topX values
- Handling of duplicate detection_oid values across the full pipeline

### Generating Test Data
For performance testing with larger datasets, you can use the TestDataGenerator utility:
```bash
sbt "runMain com.htx.util.TestDataGenerator 20 5000 10 data/
```