package org.kindofdev.data

import julienrf.json.derived
import org.kindofdev.utils.JsonFormats._
import play.api.libs.json._

import java.time.Instant
import java.util.UUID

sealed trait Event {
  def id: UUID
  def processedAt: Instant
}

sealed trait WalletEvent extends Event {
  def userId: UUID
}

object WalletEvent {
  implicit val format: Format[WalletEvent] = derived.flat.oformat((__ \ "type").format[String])
}

case class KindOfWalletEvent(
    id: UUID,
    userId: UUID,
    processedAt: Instant,
    somethingInteresting: String
) extends WalletEvent

object KindOfWalletEvent {
  implicit val format: Format[KindOfWalletEvent] = Json.format[KindOfWalletEvent]
}

sealed trait MoneyEvent extends WalletEvent {
  def amount: BigDecimal
  def currency: String
  def openingBalance: BigDecimal
  def closingBalance: BigDecimal
  def userSessionId: Option[UUID]
  def isOldMoneyEvent: Boolean
  def moneyType: String
}

object MoneyEvent {
  implicit val format: Format[MoneyEvent] = derived.flat.oformat((__ \ "type").format[String])
}

case class PaymentEvent(
    id: UUID,
    userId: UUID,
    userSessionId: Option[UUID],
    processedAt: Instant,
    amount: BigDecimal,
    currency: String,
    openingBalance: BigDecimal,
    closingBalance: BigDecimal,
    moneyType: String,
    paymentRelatedThing: String,
    isOldMoneyEvent: Boolean
) extends MoneyEvent

object PaymentEvent {
  implicit val format: Format[PaymentEvent] = Json.format[PaymentEvent]
}

case class TransactionEvent(
    id: UUID,
    userId: UUID,
    userSessionId: Option[UUID],
    processedAt: Instant,
    amount: BigDecimal,
    currency: String,
    openingBalance: BigDecimal,
    closingBalance: BigDecimal,
    moneyType: String,
    transactionRelatedThing: String,
    isOldMoneyEvent: Boolean
) extends MoneyEvent

object TransactionEvent {
  implicit val format: Format[TransactionEvent] = Json.format[TransactionEvent]
}

sealed trait UserEvent extends Event {
  def userId: UUID
}

object UserEvent {
  implicit val format: Format[UserEvent] = derived.flat.oformat((__ \ "type").format[String])
}

case class ChangeBirthDateEvent(
    id: UUID,
    userId: UUID,
    processedAt: Instant,
    birthYear: String,
    birthCityPlace: String
) extends UserEvent

object ChangeBirthDateEvent {
  implicit val format: Format[ChangeBirthDateEvent] = Json.format[ChangeBirthDateEvent]
}

case class ChangeAddressEvent(
    id: UUID,
    userId: UUID,
    processedAt: Instant,
    address: String,
    residentCountryCode: String,
    postalCode: String
) extends UserEvent

object ChangeAddressEvent {
  implicit val format: Format[ChangeAddressEvent] = Json.format[ChangeAddressEvent]
}

case class OtherUserEvent(
    id: UUID,
    userId: UUID,
    processedAt: Instant,
    somethingUtil: String
) extends UserEvent

object OtherUserEvent {
  implicit val format: Format[OtherUserEvent] = Json.format[OtherUserEvent]
}

sealed trait UserSessionEvent extends Event {
  def userId: UUID
}

object UserSessionEvent {
  implicit val format: Format[UserSessionEvent] = derived.flat.oformat((__ \ "type").format[String])
}

case class UserSessionCreatedEvent(
    id: UUID,
    userId: UUID,
    processedAt: Instant,
    clientCountryCode: String
) extends UserSessionEvent

object UserSessionCreatedEvent {
  implicit val format: Format[UserSessionCreatedEvent] = Json.format[UserSessionCreatedEvent]
}

case class UserSessionClosedEvent(
    id: UUID,
    userId: UUID,
    processedAt: Instant,
    reason: String
) extends UserSessionEvent

object UserSessionClosedEvent {
  implicit val format: Format[UserSessionClosedEvent] = Json.format[UserSessionClosedEvent]
}
