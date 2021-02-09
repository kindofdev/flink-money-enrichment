package org.kindofdev.udfs

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses
import org.kindofdev.data.{DataGenerator, _}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import java.util.UUID

class MoneyMeetsUserFunctionSpec extends AnyFlatSpec with Matchers {

  import MoneyMeetsUserFunctionSpec._

  "MoneyMeetsUserFunction" should "join money with user info when events arrive in order" in {
    val function = new MoneyMeetsUserFunction
    val harness = ProcessFunctionTestHarnesses.forKeyedCoProcessFunction[UUID, MoneyEnrichmentBuilder, UserEvent, MoneyEnrichmentBuilder](
      function,
      _.money.userId,
      _.userId,
      createTypeInformation[UUID]
    )

    val expected = money1.withUserInfo(Option(UserInfo(Option("1978"), Option("ES"))))

    harness.processElement2(changeBirthDateEvent, 10)
    harness.numKeyedStateEntries() should be(1) // userInfoBuffer
    harness.numEventTimeTimers() should be(1)

    harness.processElement2(changeAddressEvent, 20)
    harness.numEventTimeTimers() should be(2)

    harness.processElement1(money1, 30)
    harness.numKeyedStateEntries() should be(2) // userInfoBuffer + moneyBuffer
    harness.numEventTimeTimers() should be(3)

    harness.processBothWatermarks(new Watermark(40))

    harness.extractOutputValues() should contain(expected)
    harness.numKeyedStateEntries() should be(1) // userInfoState
    harness.numEventTimeTimers() should be(0)
  }

  "MoneyMeetsUserFunction" should "join money with user info when events arrive out of in order" in {
    val function = new MoneyMeetsUserFunction
    val harness = ProcessFunctionTestHarnesses.forKeyedCoProcessFunction[UUID, MoneyEnrichmentBuilder, UserEvent, MoneyEnrichmentBuilder](
      function,
      _.money.userId,
      _.userId,
      createTypeInformation[UUID]
    )

    val expected = money1.withUserInfo(Option(UserInfo(Option("1978"), Option("ES"))))

    harness.processElement1(money1, 30)
    harness.numKeyedStateEntries() should be(1) // moneyBuffer
    harness.numEventTimeTimers() should be(1)

    harness.processElement2(changeAddressEvent, 20)
    harness.numEventTimeTimers() should be(2)
    harness.numKeyedStateEntries() should be(2) // userInfoBuffer + moneyBuffer

    harness.processElement2(changeBirthDateEvent, 10)
    harness.numEventTimeTimers() should be(3)

    harness.processBothWatermarks(new Watermark(40))

    harness.extractOutputValues() should contain(expected)
    harness.numKeyedStateEntries() should be(1) // userInfoState
    harness.numEventTimeTimers() should be(0)
  }

  "MoneyMeetsUserFunction" should "join money with user info empty if there's no user events" in {
    val function = new MoneyMeetsUserFunction
    val harness = ProcessFunctionTestHarnesses.forKeyedCoProcessFunction[UUID, MoneyEnrichmentBuilder, UserEvent, MoneyEnrichmentBuilder](
      function,
      _.money.userId,
      _.userId,
      createTypeInformation[UUID]
    )

    val expected = money1.withUserInfo(None)

    harness.processElement1(money1, 30)
    harness.numKeyedStateEntries() should be(1) // moneyBuffer
    harness.numEventTimeTimers() should be(1)

    harness.processBothWatermarks(new Watermark(40))

    harness.extractOutputValues() should contain(expected)
    harness.numKeyedStateEntries() should be(0)
    harness.numEventTimeTimers() should be(0)
  }

  "MoneyMeetsUserFunction" should "join money with user info empty if user events are late" in {
    val function = new MoneyMeetsUserFunction
    val harness = ProcessFunctionTestHarnesses.forKeyedCoProcessFunction[UUID, MoneyEnrichmentBuilder, UserEvent, MoneyEnrichmentBuilder](
      function,
      _.money.userId,
      _.userId,
      createTypeInformation[UUID]
    )

    val expected1 = money1.withUserInfo(None)

    harness.processElement1(money1, 30)
    harness.numKeyedStateEntries() should be(1) // moneyBuffer
    harness.numEventTimeTimers() should be(1)

    harness.processBothWatermarks(new Watermark(40))

    harness.extractOutputValues() should contain(expected1)
    harness.numKeyedStateEntries() should be(0)
    harness.numEventTimeTimers() should be(0)

    // Late events
    harness.processElement2(changeBirthDateEvent, 10)
    harness.processElement2(changeAddressEvent, 20)
    harness.numEventTimeTimers() should be(2)
    harness.numKeyedStateEntries() should be(1) // userInfoBuffer

    // New money events should be enriched with userInfo
    val expected2 = money2.withUserInfo(Option(UserInfo(Option("1978"), Option("ES"))))

    harness.processElement1(money2, 50)
    harness.processBothWatermarks(new Watermark(60))

    harness.extractOutputValues() should contain(expected2)
    harness.numEventTimeTimers() should be(0)
    harness.numKeyedStateEntries() should be(1) // userInfoState
  }

  "MoneyMeetsUserFunction" should "join late money events" in {
    val function = new MoneyMeetsUserFunction
    val harness = ProcessFunctionTestHarnesses.forKeyedCoProcessFunction[UUID, MoneyEnrichmentBuilder, UserEvent, MoneyEnrichmentBuilder](
      function,
      _.money.userId,
      _.userId,
      createTypeInformation[UUID]
    )

    val expected = money1.withUserInfo(Option(UserInfo(Option("1978"), Option("ES"))))

    harness.processElement2(changeBirthDateEvent, 10)
    harness.numKeyedStateEntries() should be(1) // userInfoBuffer
    harness.numEventTimeTimers() should be(1)

    harness.processElement2(changeAddressEvent, 20)
    harness.numEventTimeTimers() should be(2)

    harness.processBothWatermarks(new Watermark(40))

    harness.processElement1(money1, 30) // late money

    harness.extractOutputValues() should contain(expected)
    harness.numKeyedStateEntries() should be(1) // userInfoState
    harness.numEventTimeTimers() should be(0)
  }
}

object MoneyMeetsUserFunctionSpec {

  val userId: UUID = UUID.randomUUID()
  val now: Instant = Instant.now()

  val money1: MoneyEnrichmentBuilder = MoneyEnrichmentBuilder(
    Money.fromMoneyEvent(DataGenerator.createTransactionEvent(userId = userId))
  )

  val money2: MoneyEnrichmentBuilder = MoneyEnrichmentBuilder(
    Money.fromMoneyEvent(DataGenerator.createTransactionEvent(userId = userId))
  )

  val changeBirthDateEvent: ChangeBirthDateEvent = ChangeBirthDateEvent(UUID.randomUUID(), userId, Instant.now(), "1978", "Malaga")
  val changeAddressEvent: ChangeAddressEvent = ChangeAddressEvent(UUID.randomUUID(), userId, Instant.now(), "my address", "ES", "29001")

}
