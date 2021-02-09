package org.kindofdev.data.serde

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala._
import org.kindofdev.data.UserSessionEvent
import org.kindofdev.log.Logging
import play.api.libs.json.Json

class UserSessionDeserializationSchema extends DeserializationSchema[UserSessionEvent] with Logging {

  override def deserialize(message: Array[Byte]): UserSessionEvent = Json.parse(message).as[UserSessionEvent]

  override def isEndOfStream(nextElement: UserSessionEvent): Boolean = false

  override def getProducedType: TypeInformation[UserSessionEvent] = createTypeInformation[UserSessionEvent]
}
