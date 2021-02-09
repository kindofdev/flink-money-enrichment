package org.kindofdev.udfs

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses
import org.kindofdev.data._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.{Duration, Instant}
import java.util.UUID

class MoneyMeetsSessionFunctionSpec extends AnyFlatSpec with Matchers {

  import MoneyMeetsSessionFunctionSpec._
  "MoneyMeetsSessionFunction" should "join money with session info when events arrive in order" in {
    val harness = buildHarness()
    val expected = money.withSessionInfo(Option(SessionInfo(session.clientCountryCode)))

    harness.processElement2(session, 10)
    harness.processElement1(money, 20)
    harness.processBothWatermarks(new Watermark(21))

    harness.extractOutputValues() should contain(expected)
    harness.numKeyedStateEntries() should be(1) // sessionInfoState
    harness.numEventTimeTimers() should be(1) // sessionExpirationTimer
  }

  "MoneyMeetsSessionFunction" should "join money with session info when session event is late" in {
    val harness = buildHarness()
    val expected = money.withSessionInfo(Option(SessionInfo(session.clientCountryCode)))

    harness.processElement1(money, 20)
    harness.processElement2(session, 10)
    harness.processBothWatermarks(new Watermark(21))

    harness.extractOutputValues() should contain(expected)
    harness.numKeyedStateEntries() should be(1) // sessionInfoState
    harness.numEventTimeTimers() should be(1) // sessionExpirationTimer
  }

  "MoneyMeetsSessionFunction" should "join money with session info empty if money does not have session" in {
    val harness = buildHarness()
    val expected = moneyWithoutSession.withSessionInfo(None)

    harness.processElement1(moneyWithoutSession, 20)
    harness.processBothWatermarks(new Watermark(21))

    harness.extractOutputValues() should contain(expected)
    harness.numKeyedStateEntries() should be(0)
    harness.numEventTimeTimers() should be(0)
  }

  "MoneyMeetsSessionFunction" should "forward late money events (session expired) to a 'late events side output'" in {
    val harness = buildHarness()
    val SixtyOneMinutes = Duration.ofMinutes(61).toMillis
    val FiftyNineMinutes = Duration.ofMinutes(59).toMillis

    harness.processElement2(session, 10)

    // session expires
    harness.processBothWatermarks(new Watermark(SixtyOneMinutes))

    harness.processElement1(money, FiftyNineMinutes)
    harness.extractOutputValues() should be(empty)
    harness.getSideOutput(lateMoneyOutputTag) should contain(new StreamRecord(money, FiftyNineMinutes))
    harness.numKeyedStateEntries() should be(0)
    harness.numEventTimeTimers() should be(0)
  }

  "MoneyMeetsSessionFunction" should "forward old money events (isOldMoneyEvent == true) to a 'late events side output'" in {
    val harness = buildHarness()

    harness.processElement1(oldMoney, 20)
    harness.processBothWatermarks(new Watermark(21))

    harness.extractOutputValues() should be(empty)
    harness.getSideOutput(lateMoneyOutputTag) should contain(new StreamRecord(oldMoney, 20))
    harness.numKeyedStateEntries() should be(0)
    harness.numEventTimeTimers() should be(0)
  }

  private def buildHarness() = {
    val function = new MoneyMeetsSessionFunction(lateMoneyOutputTag, sessionTTL)
    ProcessFunctionTestHarnesses
      .forKeyedCoProcessFunction[Option[UUID], MoneyEnrichmentBuilder, UserSessionCreatedEvent, MoneyEnrichmentBuilder](
        function,
        moneyBuilder => moneyBuilder.money.userSessionId,
        session => Option(session.id),
        createTypeInformation[Option[UUID]]
      )
  }

}

object MoneyMeetsSessionFunctionSpec {
  val userId: UUID = UUID.randomUUID()
  val userSessionId: UUID = UUID.randomUUID()
  val session: UserSessionCreatedEvent = UserSessionCreatedEvent(userSessionId, userId, Instant.now(), "CA")

  val money: MoneyEnrichmentBuilder = MoneyEnrichmentBuilder(
    Money.fromMoneyEvent(DataGenerator.createTransactionEvent(userId = userId, userSessionId = Option(userSessionId)))
  )

  val moneyWithoutSession: MoneyEnrichmentBuilder = MoneyEnrichmentBuilder(
    Money.fromMoneyEvent(DataGenerator.createTransactionEvent(userId = userId, userSessionId = None))
  )

  val oldMoney: MoneyEnrichmentBuilder = MoneyEnrichmentBuilder(
    Money
      .fromMoneyEvent(DataGenerator.createTransactionEvent(userId = userId, userSessionId = Option(userSessionId)))
      .copy(isOldMoneyEvent = true)
  )

  val lateMoneyOutputTag: OutputTag[MoneyEnrichmentBuilder] = OutputTag[MoneyEnrichmentBuilder]("late-money-tag")
  val sessionTTL: Duration = Duration.ofHours(1)
}
