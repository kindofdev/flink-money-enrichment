package org.kindofdev.data

import com.sksamuel.avro4s._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.kindofdev.data.avro.{AvroGenericRecordWriter, AvroSerializable, MoneyEnrichmentBuilderSerializer}

case class MoneyEnrichmentBuilder(
    money: Money,
    userInfo: Option[Option[UserInfo]] = None,
    sessionInfo: Option[Option[SessionInfo]] = None,
    firstOccurrence: Option[Boolean] = None
) extends AvroGenericRecordWriter {

  import MoneyEnrichmentBuilder._

  def isComplete: Boolean = (userInfo, sessionInfo, firstOccurrence) match {
    case (Some(_), Some(_), Some(_)) => true
    case _                           => false
  }

  def withUserInfo(userInfo: Option[UserInfo]): MoneyEnrichmentBuilder =
    copy(userInfo = Some(userInfo))

  def withSessionInfo(sessionInfo: Option[SessionInfo]): MoneyEnrichmentBuilder =
    copy(sessionInfo = Some(sessionInfo))

  def withFirstOccurrence(firstOccurrence: Boolean): MoneyEnrichmentBuilder =
    copy(firstOccurrence = Some(firstOccurrence))

  def build(): MoneyEnriched =
    if (isComplete) MoneyEnriched(money, userInfo.get, sessionInfo.get, firstOccurrence.get)
    else throw new RuntimeException("money enrichment is not complete")

  override def toGenericRecord: GenericRecord = avroFormat.to(this)
}

object MoneyEnrichmentBuilder extends org.kindofdev.data.avro.AvroSchema with AvroSerializable[MoneyEnrichmentBuilder] {

  val avroFormat: RecordFormat[MoneyEnrichmentBuilder] = RecordFormat[MoneyEnrichmentBuilder]

  def apply(record: GenericRecord): MoneyEnrichmentBuilder = avroFormat.from(record)

  override def serializer: TypeSerializer[MoneyEnrichmentBuilder] = new MoneyEnrichmentBuilderSerializer(None)

  override def getCurrentSchema: Schema = AvroSchema[MoneyEnrichmentBuilder]

}
