package org.kindofdev.data.avro

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.util.InstantiationUtil
import org.kindofdev.data.UserInfo

class UserInfoSerializer(val stateSchema: Option[Schema]) extends CustomAvroSerializer[UserInfo] {

  override def getCurrentSchema: Schema = UserInfo.getCurrentSchema

  override def fromGenericRecord(genericRecord: GenericRecord): UserInfo = UserInfo(genericRecord)

  override def duplicate(): TypeSerializer[UserInfo] = new UserInfoSerializer(stateSchema)

  override def createInstance(): UserInfo = InstantiationUtil.instantiate(classOf[UserInfo])

  override def snapshotConfiguration(): TypeSerializerSnapshot[UserInfo] = new UserInfoSerializerSnapshot()
}
