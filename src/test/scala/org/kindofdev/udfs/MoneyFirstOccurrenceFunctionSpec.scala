package org.kindofdev.udfs

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.operators.StreamMap
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.util.{KeyedOneInputStreamOperatorTestHarness, TestHarnessUtil}
import org.kindofdev.data.{DataGenerator, Money, MoneyEnrichmentBuilder}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue

class MoneyFirstOccurrenceFunctionSpec extends AnyFlatSpec with Matchers with BeforeAndAfter {

  private var testHarness: KeyedOneInputStreamOperatorTestHarness[UUID, MoneyEnrichmentBuilder, MoneyEnrichmentBuilder] = _
  private var firstOccurrenceFunction: MoneyFirstOccurrenceFunction = _

  private val userId1 = UUID.randomUUID()
  private val userId2 = UUID.randomUUID()

  private val money1User1 = MoneyEnrichmentBuilder(
    Money.fromMoneyEvent(DataGenerator.createTransactionEvent(userId = userId1, moneyType = "A"))
  )

  private val money1User2 = MoneyEnrichmentBuilder(
    Money.fromMoneyEvent(DataGenerator.createTransactionEvent(userId = userId2, moneyType = "A"))
  )

  private val money2User1 = MoneyEnrichmentBuilder(
    Money.fromMoneyEvent(DataGenerator.createTransactionEvent(userId = userId1, moneyType = "A"))
  )

  private val money3User1 = MoneyEnrichmentBuilder(
    Money.fromMoneyEvent(DataGenerator.createTransactionEvent(userId = userId1, moneyType = "B"))
  )

  before {
    firstOccurrenceFunction = new MoneyFirstOccurrenceFunction
    testHarness = new KeyedOneInputStreamOperatorTestHarness[UUID, MoneyEnrichmentBuilder, MoneyEnrichmentBuilder](
      new StreamMap[MoneyEnrichmentBuilder, MoneyEnrichmentBuilder](firstOccurrenceFunction),
      _.money.userId,
      Types.of[UUID]
    )

    testHarness.open()
  }

  "MoneyFirstOccurrenceFunction" should "detect an event has occurred for a userId" in {

    case class Foo(value: Int)

    val expectedOutput: ConcurrentLinkedQueue[Object] = new ConcurrentLinkedQueue[Object]()

    testHarness.processElement(money1User1, 100)
    expectedOutput.add(new StreamRecord(money1User1.withFirstOccurrence(true), 100))

    testHarness.processElement(money1User2, 200)
    expectedOutput.add(new StreamRecord(money1User2.withFirstOccurrence(true), 200))

    testHarness.processElement(money2User1, 300)
    expectedOutput.add(new StreamRecord(money2User1.withFirstOccurrence(false), 300))

    testHarness.processElement(money3User1, 400)
    expectedOutput.add(new StreamRecord(money3User1.withFirstOccurrence(true), 400))

    TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput)
  }

}
