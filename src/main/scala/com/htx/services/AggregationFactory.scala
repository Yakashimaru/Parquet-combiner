// HTX Data Engineering Assessment
// Created by: Joel John Tan
// Date: March 2025

package com.htx.services

import com.htx.models.Models.{ResultBase}
import com.htx.services.Aggregations.{
  TopItemsAggregation,
  ItemCountAggregation,
  LocationStatsAggregation
}

/** Factory for creating aggregation operations.
  *
  * This factory implements the Factory Method design pattern, which:
  *   1. Decouples client code from concrete aggregation implementations 2.
  *      Centralizes instantiation logic in one place 3. Makes it easy to add
  *      new aggregation types without modifying client code
  *
  * The design enables runtime selection of different aggregation strategies
  * while maintaining type safety through Scala's type system.
  *
  * For geographical data aggregation, this means we can easily add new metrics
  * or analysis methods grouped by geographical_location_oid without changing
  * the core processing pipeline.
  */

object AggregationFactory {
  def createAggregation(name: String): AggregationOperation[_ <: ResultBase] =
    name match {
      case "topItems"      => new TopItemsAggregation()
      case "itemCount"     => new ItemCountAggregation()
      case "locationStats" => new LocationStatsAggregation()
      case _ =>
        throw new IllegalArgumentException(s"Unknown aggregation type: $name")
    }
}
