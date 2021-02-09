package org.kindofdev.data.avro

import org.apache.avro.Schema
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.kindofdev.data.UserInfo

class UserInfoSerializerSnapshot(var stateSchema: Option[Schema]) extends CustomAvroSerializerSnapshot[UserInfo] {

  def this() = {
    this(None)
  }

  override def getCurrentSchema: Schema = UserInfo.getCurrentSchema

  override def restoreSerializer(): TypeSerializer[UserInfo] = new UserInfoSerializer(stateSchema)
}
