package org.kindofdev.data.avro

import org.apache.avro.generic.GenericRecord

trait AvroGenericRecordWriter {
  def toGenericRecord: GenericRecord
}
