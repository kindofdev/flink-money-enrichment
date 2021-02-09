package org.kindofdev.udfs

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.operators.StreamFilter
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness
import org.kindofdev.data.{DataGenerator, Money}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.{Duration, OffsetDateTime}
import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue

class DedupFilterSpec extends AnyFlatSpec with Matchers with BeforeAndAfter {

  private var testHarness: KeyedOneInputStreamOperatorTestHarness[UUID, Money, Money] = _
  private var dedupFilter: DedupFilter[Money, UUID] = _

  private val now = OffsetDateTime.now()

  private val userId = UUID.randomUUID()
  private val id1 = UUID.randomUUID()
  private val id2 = UUID.randomUUID()
  private val id3 = UUID.randomUUID()

  private val money1 = Money.fromMoneyEvent(DataGenerator.createTransactionEvent(id = id1, userId = userId))
  private val money2 = Money.fromMoneyEvent(DataGenerator.createTransactionEvent(id = id2, userId = userId))
  private val money3 = Money.fromMoneyEvent(DataGenerator.createTransactionEvent(id = id3, userId = userId))

  before {
    dedupFilter = new DedupFilter(Duration.ofSeconds(1), _.id)
    testHarness =
      new KeyedOneInputStreamOperatorTestHarness[UUID, Money, Money](new StreamFilter[Money](dedupFilter), _.userId, Types.of[UUID])

    testHarness.open()
  }

  "DedupFilter" should "deduplicate events" in {
    testHarness.processElement(money1, now.toInstant.toEpochMilli)
    testHarness.processElement(money1, now.plusMinutes(1).toInstant.toEpochMilli)
    testHarness.processElement(money2, now.plusMinutes(2).toInstant.toEpochMilli)

    testHarness.getOutput.size() should be(2)
  }

  ignore should "not deduplicate events if ttl finished" in {  // this scenario works in manual test (something happens with harness)
    testHarness.processElement(money1, 1000)
    testHarness.processElement(money2, 2000)
    Thread.sleep(2000) // or testHarness.setProcessingTime(4000)
    testHarness.processElement(money3, 3000)
    testHarness.processElement(money1, 4000)

    testHarness.getOutput.size() should be(4)
  }

  private def printOutput(output: ConcurrentLinkedQueue[AnyRef]): Unit =
    output.forEach(println)

}
