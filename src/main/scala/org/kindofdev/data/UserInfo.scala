package org.kindofdev.data

import com.sksamuel.avro4s.{AvroSchema, RecordFormat}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.kindofdev.data.avro.{AvroGenericRecordWriter, AvroSchema, AvroSerializable, UserInfoSerializer}
import play.api.libs.json.{Format, Json}

case class UserInfo(
    birthYear: Option[String],
    residentCountryCode: Option[String]
) extends AvroGenericRecordWriter {

  import UserInfo._

  def isComplete: Boolean = (birthYear, residentCountryCode) match {
    case (Some(_), Some(_)) => true
    case _                  => false
  }

  def combine(other: UserInfo): UserInfo = {
    UserInfo(
      other.birthYear.orElse(birthYear),
      other.residentCountryCode.orElse(residentCountryCode)
    )
  }

  override def toGenericRecord: GenericRecord = avroFormat.to(this)
}

object UserInfo extends AvroSchema with AvroSerializable[UserInfo] {

  implicit val format: Format[UserInfo] = Json.format[UserInfo]

  val avroFormat: RecordFormat[UserInfo] = RecordFormat[UserInfo]

  def empty: UserInfo = UserInfo(None, None)

  def apply(record: GenericRecord): UserInfo = avroFormat.from(record)

  override def serializer: TypeSerializer[UserInfo] = new UserInfoSerializer(None)

  override def getCurrentSchema: Schema = AvroSchema[UserInfo]

}
