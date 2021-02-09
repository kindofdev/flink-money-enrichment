package org.kindofdev.data

import com.vitorsvieira.iso.ISOCurrency

import java.time.Instant
import java.util.UUID

object DataGenerator {

  def createTransactionEvent(
      userId: UUID = UUID.randomUUID(),
      processedAt: Instant = Instant.now(),
      userSessionId: Option[UUID] = Option(UUID.randomUUID()),
      id: UUID = UUID.randomUUID(),
      moneyType: String = "TransactionA"
  ): TransactionEvent = TransactionEvent(
    id = id,
    userId = userId,
    userSessionId = userSessionId,
    processedAt = processedAt,
    amount = 10,
    currency = ISOCurrency.EURO.value,
    openingBalance = 5,
    closingBalance = 15,
    moneyType = moneyType,
    transactionRelatedThing = "some info",
    isOldMoneyEvent = false
  )

  def createUserSessionCreatedEvent(
      userId: UUID = UUID.randomUUID(),
      processedAt: Instant = Instant.now(),
      clientCountryCode: String = "IN",
      id: UUID = UUID.randomUUID()
  ): UserSessionCreatedEvent = UserSessionCreatedEvent(
    id = id,
    userId = userId,
    processedAt = processedAt,
    clientCountryCode = clientCountryCode
  )

  def createChangeBirthDateEvent(
      userId: UUID = UUID.randomUUID(),
      processedAt: Instant = Instant.now(),
      birthYear: String
  ): ChangeBirthDateEvent = ChangeBirthDateEvent(
    id = UUID.randomUUID(),
    userId = userId,
    processedAt = processedAt,
    birthYear = birthYear,
    birthCityPlace = "MA"
  )

  def createChangeAddressEvent(
      userId: UUID = UUID.randomUUID(),
      processedAt: Instant = Instant.now(),
      residenceCountryCode: String = "FR"
  ): ChangeAddressEvent = ChangeAddressEvent(
    id = UUID.randomUUID(),
    userId = userId,
    processedAt = processedAt,
    address = "street A, Malaga",
    residentCountryCode = residenceCountryCode,
    postalCode = "29102"
  )

}
