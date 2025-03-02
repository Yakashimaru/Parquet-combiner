// HTX Data Engineering Assessment
// Created by: Joel John Tan
// Date: March 2025

// Logging.scala
package com.htx.utils

import org.slf4j.{Logger, LoggerFactory}

trait Logging {
  // Use lazy val to initialize the logger only when needed
  protected lazy val logger: Logger = LoggerFactory.getLogger(getClass)

}
