package org.kindofdev.data

import com.sksamuel.avro4s._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.kindofdev.data.avro.{AvroGenericRecordWriter, AvroSchema, AvroSerializable, MoneySerializer}
import play.api.libs.json.{Format, Json}

import java.time.Instant
import java.util.UUID

case class Money(
    id: UUID,
    userId: UUID,
    userSessionId: Option[UUID],
    processedAt: Instant,
    amount: BigDecimal,
    currency: String,
    closingBalance: BigDecimal,
    moneyType: String,
    isOldMoneyEvent: Boolean
) extends AvroGenericRecordWriter {

  import Money._

  override def toGenericRecord: GenericRecord = avroFormat.to(this)
}

object Money extends AvroSchema with AvroSerializable[Money] {
  import org.kindofdev.utils.JsonFormats._

  implicit val format: Format[Money] = Json.format[Money]

  val avroFormat: RecordFormat[Money] = RecordFormat[Money]

  def apply(record: GenericRecord): Money = avroFormat.from(record)

  override def serializer: TypeSerializer[Money] = new MoneySerializer(None)

  override def getCurrentSchema: Schema = AvroSchema[Money]

  def fromMoneyEvent(moneyEvent: MoneyEvent): Money =
    Money(
      id = moneyEvent.id,
      userId = moneyEvent.userId,
      userSessionId = moneyEvent.userSessionId,
      processedAt = moneyEvent.processedAt,
      amount = moneyEvent.amount,
      currency = moneyEvent.currency,
      closingBalance = moneyEvent.closingBalance,
      moneyType = moneyEvent.moneyType,
      isOldMoneyEvent = moneyEvent.isOldMoneyEvent
    )

}
