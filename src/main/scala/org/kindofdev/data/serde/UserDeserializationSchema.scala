package org.kindofdev.data.serde

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala._
import org.kindofdev.data.UserEvent
import org.kindofdev.log.Logging
import play.api.libs.json.Json

class UserDeserializationSchema extends DeserializationSchema[UserEvent] with Logging {

  override def deserialize(message: Array[Byte]): UserEvent = Json.parse(message).as[UserEvent]

  override def isEndOfStream(nextElement: UserEvent): Boolean = false

  override def getProducedType: TypeInformation[UserEvent] = createTypeInformation[UserEvent]
}
