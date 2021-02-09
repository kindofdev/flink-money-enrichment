package org.kindofdev.data.avro

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.util.InstantiationUtil
import org.kindofdev.data.{UserInfo, SessionInfo}

class SessionInfoSerializer (val stateSchema: Option[Schema]) extends CustomAvroSerializer[SessionInfo] {

  override def getCurrentSchema: Schema = SessionInfo.getCurrentSchema

  override def fromGenericRecord(genericRecord: GenericRecord): SessionInfo = SessionInfo(genericRecord)

  override def duplicate(): TypeSerializer[SessionInfo] = new SessionInfoSerializer(stateSchema)

  override def createInstance(): SessionInfo = InstantiationUtil.instantiate(classOf[SessionInfo])

  override def snapshotConfiguration(): TypeSerializerSnapshot[SessionInfo] = new SessionInfoSerializerSnapshot()
}
