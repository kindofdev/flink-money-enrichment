package org.kindofdev.data.serde

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala._
import org.kindofdev.data.WalletEvent
import org.kindofdev.log.Logging
import play.api.libs.json.Json

class WalletDeserializationSchema extends DeserializationSchema[WalletEvent] with Logging {

  override def deserialize(message: Array[Byte]): WalletEvent = Json.parse(message).as[WalletEvent]

  override def isEndOfStream(nextElement: WalletEvent): Boolean = false

  override def getProducedType: TypeInformation[WalletEvent] = createTypeInformation[WalletEvent]
}
