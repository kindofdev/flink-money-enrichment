package org.kindofdev.data.avro

import org.apache.avro.Schema
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.kindofdev.data.MoneyEnrichmentBuilder

class MoneyEnrichmentBuilderSerializerSnapshot(var stateSchema: Option[Schema])
    extends CustomAvroSerializerSnapshot[MoneyEnrichmentBuilder] {

  def this() = {
    this(None)
  }

  override def getCurrentSchema: Schema = MoneyEnrichmentBuilder.getCurrentSchema

  override def restoreSerializer(): TypeSerializer[MoneyEnrichmentBuilder] = new MoneyEnrichmentBuilderSerializer(stateSchema)
}
