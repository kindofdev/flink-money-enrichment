package org.kindofdev.data.serde

import org.apache.flink.api.common.serialization.SerializationSchema
import org.kindofdev.data.MoneyEnriched
import play.api.libs.json.Json

class MoneyEnrichedSerializationSchema extends SerializationSchema[MoneyEnriched] {
  override def serialize(element: MoneyEnriched): Array[Byte] =
    Json.toBytes(Json.toJson(element))
}
