package org.kindofdev

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.extensions._
import org.apache.flink.streaming.api.scala.{AsyncDataStream, DataStream, StreamExecutionEnvironment}
import org.kindofdev.data._
import org.kindofdev.io.{IORepo, SessionHttpClient}
import org.kindofdev.udfs._

import java.util.UUID
import java.util.concurrent.TimeUnit

// TODO - Readme

class MoneyEnrichmentJob(ioRepo: IORepo, sessionHttpClient: SessionHttpClient, settings: Settings)(
    implicit env: StreamExecutionEnvironment
) {

  private type IsFirstOccurrence = Boolean

  def execute(): Unit = {

    val moneyStream: DataStream[Money] = getMoneyStream
    val userStream: DataStream[UserEvent] = getUserStream
    val sessionCreatedStream: DataStream[UserSessionCreatedEvent] = getSessionCreatedStream

    val moneyDedup: DataStream[Money] = deduplicateMoney(moneyStream)
    val moneyEnrichment: DataStream[MoneyEnrichmentBuilder] =
      moneyDedup
        .map(MoneyEnrichmentBuilder(_))
        .nameAndUid("money-enrichment-builder")

    val moneyEnrichedWithSession = enrichWithSession(sessionCreatedStream, moneyEnrichment)

    val moneyEnrichedWithUser: DataStream[MoneyEnrichmentBuilder] =
      enrichWithUser(userStream, moneyEnrichedWithSession)

    val moneyEnrichedWithFirstOccurrence: DataStream[MoneyEnrichmentBuilder] =
      enrichWithFirstOccurrence(moneyEnrichedWithUser)

    val moneyFullyEnriched: DataStream[MoneyEnriched] =
      moneyEnrichedWithFirstOccurrence
        .map(_.build())
        .nameAndUid("map-build-money-enriched")

    moneyFullyEnriched
      .addSink(ioRepo.moneyEnrichedSinkFunction)
      .name("sink-money-enriched")
      .uid("sink-money-enriched")

    env.execute("Money enrichment app")
  }

  private def enrichWithFirstOccurrence(moneyStream: DataStream[MoneyEnrichmentBuilder]): DataStream[MoneyEnrichmentBuilder] =
    moneyStream
      .keyBy(_.money.userId)
      .map(new MoneyFirstOccurrenceFunction)
      .nameAndUid("map-money-first-occurrence")

  private def enrichWithUser(
      userStream: DataStream[UserEvent],
      moneyEnrichmentBuilderStream: DataStream[MoneyEnrichmentBuilder]
  ): DataStream[MoneyEnrichmentBuilder] =
    moneyEnrichmentBuilderStream
      .connect(userStream)
      .keyBy(_.money.userId, _.userId)
      .process(new MoneyMeetsUserFunction)
      .nameAndUid("join-money-user")

  private def enrichWithSession(
      sessionCreatedStream: DataStream[UserSessionCreatedEvent],
      moneyEnrichmentStream: DataStream[MoneyEnrichmentBuilder]
  ): DataStream[MoneyEnrichmentBuilder] = {

    val moneyMeetsSessionStream =
      moneyEnrichmentStream
        .connect(sessionCreatedStream)
        .keyBy(_.money.userSessionId, session => Option(session.id))
        .process(new MoneyMeetsSessionFunction(settings.lateMoneyOutputTag, settings.sessionTTL))
        .nameAndUid("join-money-session")

    val lateMoneyStream: DataStream[MoneyEnrichmentBuilder] =
      moneyMeetsSessionStream.getSideOutput(settings.lateMoneyOutputTag)

    val moneyMeetsSessionAsyncStream: DataStream[MoneyEnrichmentBuilder] =
      AsyncDataStream
        .orderedWait(
          lateMoneyStream,
          new AsyncMoneyMeetsSessionFunction(sessionHttpClient),
          settings.asyncSessionRequestTimeoutInMillis,
          TimeUnit.MILLISECONDS,
          100
        )
        .nameAndUid("join-money-session-async")

    moneyMeetsSessionStream union moneyMeetsSessionAsyncStream
  }

  private def deduplicateMoney(moneyStream: DataStream[Money]): DataStream[Money] =
    moneyStream
      .keyBy(_.userId)
      .filter(new DedupFilter[Money, UUID](settings.dedupMoneyTTL, _.id))
      .nameAndUid("dedup-money")

  private def getUserStream: DataStream[UserEvent] = {
    env
      .addSource(ioRepo.userEventSourceFunction)
      .nameAndUid("source-user")
      .filterWith {
        case _: ChangeAddressEvent | _: ChangeBirthDateEvent => true
        case _                                               => false
      }
      .nameAndUid("filter-user")
  }

  private def getMoneyStream: DataStream[Money] = {
    env
      .addSource(ioRepo.walletEventSourceFunction)
      .nameAndUid("source-money")
      .flatMapWith {
        case moneyEvent: MoneyEvent => Seq(Money.fromMoneyEvent(moneyEvent))
        case _                      => Seq.empty
      }
      .nameAndUid("filter-money")
  }

  private def getSessionCreatedStream: DataStream[UserSessionCreatedEvent] = {
    env
      .addSource(ioRepo.userSessionEventSourceFunction)
      .nameAndUid("source-session")
      .flatMapWith {
        case event: UserSessionCreatedEvent => Seq(event)
        case _                              => Seq.empty
      }
      .nameAndUid("filter-session")
  }

}
