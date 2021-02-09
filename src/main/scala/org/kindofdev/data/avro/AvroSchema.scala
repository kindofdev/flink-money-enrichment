package org.kindofdev.data.avro

import org.apache.avro.Schema

trait AvroSchema {
  def getCurrentSchema: Schema
}
