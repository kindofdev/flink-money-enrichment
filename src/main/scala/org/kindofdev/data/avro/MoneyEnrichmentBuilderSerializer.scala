package org.kindofdev.data.avro

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.util.InstantiationUtil
import org.kindofdev.data.MoneyEnrichmentBuilder

class MoneyEnrichmentBuilderSerializer(val stateSchema: Option[Schema]) extends CustomAvroSerializer[MoneyEnrichmentBuilder] {

  override def getCurrentSchema: Schema = MoneyEnrichmentBuilder.getCurrentSchema

  override def fromGenericRecord(genericRecord: GenericRecord): MoneyEnrichmentBuilder = MoneyEnrichmentBuilder(genericRecord)

  override def duplicate(): TypeSerializer[MoneyEnrichmentBuilder] = new MoneyEnrichmentBuilderSerializer(stateSchema)

  override def createInstance(): MoneyEnrichmentBuilder = InstantiationUtil.instantiate(classOf[MoneyEnrichmentBuilder])

  override def snapshotConfiguration(): TypeSerializerSnapshot[MoneyEnrichmentBuilder] = new MoneyEnrichmentBuilderSerializerSnapshot()
}
