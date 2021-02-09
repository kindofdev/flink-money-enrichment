package org.kindofdev.data.avro

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.util.InstantiationUtil
import org.kindofdev.data.Money

class MoneySerializer(val stateSchema: Option[Schema]) extends CustomAvroSerializer[Money] {

  override def getCurrentSchema: Schema = Money.getCurrentSchema

  override def fromGenericRecord(genericRecord: GenericRecord): Money = Money(genericRecord)

  override def duplicate(): TypeSerializer[Money] = new MoneySerializer(stateSchema)

  override def createInstance(): Money = InstantiationUtil.instantiate(classOf[Money])

  override def snapshotConfiguration(): TypeSerializerSnapshot[Money] = new MoneySerializerSnapshot()
}
