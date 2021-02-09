package org.kindofdev.data.avro

import org.apache.avro.Schema
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.kindofdev.data.Money

class MoneySerializerSnapshot(var stateSchema: Option[Schema]) extends CustomAvroSerializerSnapshot[Money] {

  def this() = {
    this(None)
  }

  override def getCurrentSchema: Schema = Money.getCurrentSchema

  override def restoreSerializer(): TypeSerializer[Money] = new MoneySerializer(stateSchema)
}
