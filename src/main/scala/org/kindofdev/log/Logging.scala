package org.kindofdev.log

import org.slf4j.{Logger, LoggerFactory}

trait Logging {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(this.getClass)
}
