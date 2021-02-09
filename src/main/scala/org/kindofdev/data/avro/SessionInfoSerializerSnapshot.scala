package org.kindofdev.data.avro

import org.apache.avro.Schema
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.kindofdev.data.SessionInfo

class SessionInfoSerializerSnapshot(var stateSchema: Option[Schema]) extends CustomAvroSerializerSnapshot[SessionInfo] {

  def this() = {
    this(None)
  }

  override def getCurrentSchema: Schema = SessionInfo.getCurrentSchema

  override def restoreSerializer(): TypeSerializer[SessionInfo] = new SessionInfoSerializer(stateSchema)
}
