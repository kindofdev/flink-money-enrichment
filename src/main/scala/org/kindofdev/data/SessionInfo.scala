package org.kindofdev.data

import com.sksamuel.avro4s.{AvroSchema, RecordFormat}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.kindofdev.data.avro.{AvroGenericRecordWriter, AvroSchema, AvroSerializable, SessionInfoSerializer}
import play.api.libs.json.{Format, Json}

case class SessionInfo(clientCountryCode: String) extends AvroGenericRecordWriter {
  import SessionInfo._

  override def toGenericRecord: GenericRecord = avroFormat.to(this)
}

object SessionInfo extends AvroSchema with AvroSerializable[SessionInfo] {

  implicit val format: Format[SessionInfo] = Json.format[SessionInfo]

  val avroFormat: RecordFormat[SessionInfo] =  RecordFormat[SessionInfo]

  def apply(genericRecord: GenericRecord): SessionInfo = avroFormat.from(genericRecord)

  override def getCurrentSchema: Schema = AvroSchema[SessionInfo]

  override def serializer: TypeSerializer[SessionInfo] = new SessionInfoSerializer(None)
}
