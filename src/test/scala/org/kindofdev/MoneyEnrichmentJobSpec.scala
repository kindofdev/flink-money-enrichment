package org.kindofdev

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.kindofdev.data._
import org.kindofdev.io.SessionHttpClient.SessionHttpClientFake
import org.kindofdev.util.TestIORepo.{Events, Watermark}
import org.kindofdev.util.{CollectSink, TestIORepo}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.{Duration, Instant}
import java.util.UUID
import java.util.concurrent.TimeoutException
import scala.concurrent.duration.{DurationInt, Duration => SDuration}

class MoneyEnrichmentJobSpec extends AnyFlatSpec with Matchers with BeforeAndAfter {

  import MoneyEnrichmentJobSpec._

  val flinkCluster = new MiniClusterWithClientResource(
    new MiniClusterResourceConfiguration.Builder().setNumberSlotsPerTaskManager(2).setNumberTaskManagers(1).build()
  )

  private val collectSink = new CollectSink

  before {
    flinkCluster.before()
  }

  after {
    flinkCluster.after()
  }

  implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(2)

  "MoneyEnrichmentJob" should "enrich late money events with session info from a external service" in {
    val ioRepo = new TestIORepo(LateMoney.events, collectSink)
    createJob(ioRepo).execute()

    val moneyEnriched = MoneyEnriched(
      Money.fromMoneyEvent(LateMoney.moneyEvent),
      Option(UserInfo(Option("1978"), Option("ES"))),
      Option(SessionInfo("UK")),
      firstOccurrence = true
    )

    CollectSink.values should contain(moneyEnriched)
  }

  "MoneyEnrichmentJob" should "enrich money events with user and session info" in {
    val ioRepo = new TestIORepo(HappyPath.events, collectSink)
    createJob(ioRepo).execute()

    val user1Money1Enriched = MoneyEnriched(
      Money.fromMoneyEvent(HappyPath.user1.moneyEvent1),
      Option(UserInfo(Option("1978"), Option("ES"))),
      Option(SessionInfo("CA")),
      firstOccurrence = true
    )

    val user1Money2Enriched = MoneyEnriched(
      Money.fromMoneyEvent(HappyPath.user1.moneyEvent2),
      Option(UserInfo(Option("1978"), Option("UY"))),
      Option(SessionInfo("CA")),
      firstOccurrence = false
    )

    val user2Money1Enriched = MoneyEnriched(
      Money.fromMoneyEvent(HappyPath.user2.moneyEvent1),
      Option(UserInfo(Option("1980"), Option("US"))),
      Option(SessionInfo("IT")),
      firstOccurrence = true
    )

    val user2Money2Enriched = MoneyEnriched(
      Money.fromMoneyEvent(HappyPath.user2.moneyEvent2),
      Option(UserInfo(Option("1980"), Option("US"))),
      Option(SessionInfo("IT")),
      firstOccurrence = false
    )

    CollectSink.values should contain allOf (user1Money1Enriched, user1Money2Enriched, user2Money1Enriched, user2Money2Enriched)
  }

  "MoneyEnrichmentJob" should "enrich duplicated money events only once" in {
    val ioRepo = new TestIORepo(Deduplication.events, collectSink)
    createJob(ioRepo).execute()

    val moneyEnriched = MoneyEnriched(
      Money.fromMoneyEvent(Deduplication.moneyEvent),
      Option(UserInfo(Option("1978"), Option("ES"))),
      Option(SessionInfo("CA")),
      firstOccurrence = true
    )

    CollectSink.values should contain oneElementOf Seq(moneyEnriched)
  }

  ignore should "fail due to a timeout in async session enrichment" in {
    val ioRepo = new TestIORepo(LateMoney.events, collectSink)
    val job = createJob(ioRepo, sessionAsyncRespTime = 1000.millis)

    assertThrows[TimeoutException] {
      job.execute()
    }
  }

  private def createJob(ioRepo: TestIORepo, sessionAsyncRespTime: SDuration = 100.millis): MoneyEnrichmentJob = {
    val config: Config = ConfigFactory.load()
    val settings: Settings = new Settings(config)
    val job = new MoneyEnrichmentJob(ioRepo, new SessionHttpClientFake("UK", sessionAsyncRespTime), settings)
    job
  }

}

object MoneyEnrichmentJobSpec {

  import DataGenerator._

  object HappyPath {

    object user1 {
      val userId: UUID = UUID.randomUUID()
      val userSessionId: UUID = UUID.randomUUID()

      val sessionEvent: UserSessionEvent =
        createUserSessionCreatedEvent(userId, Instant.ofEpochMilli(10), "CA", id = userSessionId)
      val addressEvent1: ChangeAddressEvent = createChangeAddressEvent(userId, Instant.ofEpochMilli(20), "ES")
      val birthEvent: ChangeBirthDateEvent = createChangeBirthDateEvent(userId, Instant.ofEpochMilli(30), "1978")
      val moneyEvent1: TransactionEvent = createTransactionEvent(userId, Instant.ofEpochMilli(40), Option(userSessionId))
      val addressEvent2: ChangeAddressEvent = createChangeAddressEvent(userId, Instant.ofEpochMilli(50), "UY")
      val moneyEvent2: TransactionEvent = createTransactionEvent(userId, Instant.ofEpochMilli(60), Option(userSessionId))
    }

    object user2 {
      val userId: UUID = UUID.randomUUID()
      val userSessionId: UUID = UUID.randomUUID()

      val sessionEvent: UserSessionEvent =
        createUserSessionCreatedEvent(userId, Instant.ofEpochMilli(11), "IT", id = userSessionId)
      val addressEvent: ChangeAddressEvent = createChangeAddressEvent(userId, Instant.ofEpochMilli(21), "US")
      val birthEvent: ChangeBirthDateEvent = createChangeBirthDateEvent(userId, Instant.ofEpochMilli(31), "1980")
      val moneyEvent1: TransactionEvent = createTransactionEvent(userId, Instant.ofEpochMilli(41), Option(userSessionId))
      val moneyEvent2: TransactionEvent = createTransactionEvent(userId, Instant.ofEpochMilli(61), Option(userSessionId))
    }

    val events: Events = Events(
      sessionEvents = Seq(
        Right(user1.sessionEvent),
        Right(user2.sessionEvent)
      ),
      userEvents = Seq(
        Right(user1.addressEvent1),
        Right(user1.birthEvent),
        Right(user2.addressEvent),
        Right(user2.birthEvent),
        Right(user1.addressEvent2)
      ),
      walletEvents = Seq(
        Right(user1.moneyEvent1),
        Right(user2.moneyEvent1),
        Right(user1.moneyEvent2),
        Right(user2.moneyEvent2)
      )
    )
  }

  object Deduplication {
    val userId: UUID = UUID.randomUUID()
    val userSessionId: UUID = UUID.randomUUID()

    val OneMinuteAndFiveSeconds: Long = Duration.ofSeconds(65).toMillis

    val sessionEvent: UserSessionEvent =
      createUserSessionCreatedEvent(userId, Instant.ofEpochMilli(10), "CA", id = userSessionId)
    val addressEvent: ChangeAddressEvent = createChangeAddressEvent(userId, Instant.ofEpochMilli(20), "ES")
    val birthEvent: ChangeBirthDateEvent = createChangeBirthDateEvent(userId, Instant.ofEpochMilli(30), "1978")
    val moneyEvent: TransactionEvent = createTransactionEvent(userId, Instant.ofEpochMilli(40), Option(userSessionId))

    val events: Events = Events(
      sessionEvents = Seq(
        Right(sessionEvent)
      ),
      userEvents = Seq(
        Right(addressEvent),
        Right(birthEvent)
      ),
      walletEvents = Seq(
        Right(moneyEvent),
        Right(moneyEvent) // duplicated event
      )
    )
  }

  object LateMoney {

    val userId: UUID = UUID.randomUUID()
    val userSessionId: UUID = UUID.randomUUID()
    val FiftyNineMinutes: Long = Duration.ofMinutes(59).toMillis
    val SixtyOneMinutes: Long = Duration.ofMinutes(61).toMillis

    // events
    val sessionEvent: UserSessionCreatedEvent =
      createUserSessionCreatedEvent(userId, Instant.ofEpochMilli(10), "CA", id = userSessionId)
    val addressEvent1: ChangeAddressEvent = createChangeAddressEvent(userId, Instant.ofEpochMilli(20), "ES")
    val birthEvent: ChangeBirthDateEvent = createChangeBirthDateEvent(userId, Instant.ofEpochMilli(30), "1978")

    val moneyEvent: TransactionEvent = createTransactionEvent(
      userId,
      Instant.ofEpochMilli(FiftyNineMinutes),
      Option(userSessionId),
      id = UUID.fromString("6fe111bb-ce66-48b9-92e6-8ac46b2551f7")
    )

    val events: Events = Events(
      sessionEvents = Seq(
        Right(sessionEvent),
        Left(Watermark(SixtyOneMinutes))
      ),
      userEvents = Seq(
        Right(addressEvent1),
        Right(birthEvent),
        Left(Watermark(SixtyOneMinutes))
      ),
      walletEvents = Seq(
        Left(Watermark(SixtyOneMinutes)),
        Right(moneyEvent)
      )
    )

  }

  object JobFailTimeout {

    val userId: UUID = UUID.randomUUID()
    val userSessionId: UUID = UUID.randomUUID()
    val FiftyNineMinutes: Long = Duration.ofMinutes(59).toMillis
    val SixtyOneMinutes: Long = Duration.ofMinutes(61).toMillis

    // events
    val sessionEvent: UserSessionCreatedEvent =
      createUserSessionCreatedEvent(userId, Instant.ofEpochMilli(10), "CA", id = userSessionId)
    val addressEvent1: ChangeAddressEvent = createChangeAddressEvent(userId, Instant.ofEpochMilli(20), "ES")
    val birthEvent: ChangeBirthDateEvent = createChangeBirthDateEvent(userId, Instant.ofEpochMilli(30), "1978")

    val moneyEvent: TransactionEvent = createTransactionEvent(
      userId,
      Instant.ofEpochMilli(FiftyNineMinutes),
      Option(userSessionId),
      id = UUID.fromString("6fe111bb-ce66-48b9-92e6-8ac46b2551f7")
    )

    val events: Events = Events(
      sessionEvents = Seq(
        Right(sessionEvent),
        Left(Watermark(SixtyOneMinutes))
      ),
      userEvents = Seq(
        Right(addressEvent1),
        Right(birthEvent),
        Left(Watermark(SixtyOneMinutes))
      ),
      walletEvents = Seq(
        Left(Watermark(SixtyOneMinutes)),
        Right(moneyEvent)
      )
    )
  }
}
