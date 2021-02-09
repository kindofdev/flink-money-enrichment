package org.kindofdev.data.avro

import org.apache.flink.api.common.typeutils.TypeSerializer

trait AvroSerializable[T] {
  def serializer: TypeSerializer[T]
}
