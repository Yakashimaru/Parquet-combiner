// HTX Data Engineering Assessment
// Created by: Joel John Tan
// Date: March 2025

package com.htx.services

import org.apache.spark.rdd.RDD
import com.htx.models.Models._

/** Base trait for all aggregation operations.
  *
  * This trait implements the Strategy pattern, defining a common interface for
  * different aggregation algorithms. The generic type parameter T ensures type
  * safety while allowing different result types.
  *
  * Key benefits for geographical data aggregation:
  *   1. Common interface for all geographic grouping operations 
  *   2. Type safety through generic parameterization 3. Promotes code reusability 
  *      by standardizing method signature
  *
  * Each implementation can reuse common code patterns for deduplication and
  * geographic grouping while implementing specific aggregation logic.
  *
  * @tparam T
  *   The result type of the aggregation, must extend ResultBase
  */

trait AggregationOperation[T <: ResultBase] extends Serializable {
  def aggregate(
      dataA: RDD[DataA],
      dataB: RDD[DataB],
      params: Map[String, Any]
  ): RDD[T]
}
