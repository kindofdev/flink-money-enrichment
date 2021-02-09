package org.kindofdev.udfs

import org.apache.flink.api.common.state.{ListStateDescriptor, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.kindofdev.data.{MoneyEnrichmentBuilder, UserSessionCreatedEvent, SessionInfo}

import java.time.Duration
import java.util.UUID
import scala.collection.JavaConverters._

class MoneyMeetsSessionFunction(lateMoneyOutputTag: OutputTag[MoneyEnrichmentBuilder], sessionTTL: Duration)
    extends MoneyMeetsOtherAbstractFunction[Option[UUID], UserSessionCreatedEvent] {

  import MoneyMeetsSessionFunction._

  private val sessionInfoDesc: ValueStateDescriptor[SessionInfo] =
    new ValueStateDescriptor[SessionInfo](SessionInfoDesc, SessionInfo.serializer)
  sessionInfoDesc.setQueryable(SessionInfoQN)
  private lazy val sessionInfoState = getRuntimeContext.getState(sessionInfoDesc)

  val pendingMoneyDueToLateSessionDesc =
    new ListStateDescriptor[MoneyEnrichmentBuilder](PendingMoneyDesc, MoneyEnrichmentBuilder.serializer)
  pendingMoneyDueToLateSessionDesc.setQueryable(PendingMoneyQN)
  private lazy val pendingMoneyDueToLateSessionState =
    getRuntimeContext.getListState[MoneyEnrichmentBuilder](pendingMoneyDueToLateSessionDesc)

  override def processElement2(
                                value: UserSessionCreatedEvent,
                                ctx: KeyedCoProcessFunction[Option[UUID], MoneyEnrichmentBuilder, UserSessionCreatedEvent, MoneyEnrichmentBuilder]#Context,
                                out: Collector[MoneyEnrichmentBuilder]
  ): Unit = {
    val sessionInfo = SessionInfo(clientCountryCode = value.clientCountryCode)
    sessionInfoState.update(sessionInfo)

    val sessionExpirationTimer = ctx.timestamp() + sessionTTL.toMillis
    ctx.timerService().registerEventTimeTimer(sessionExpirationTimer)

    // Enrich pending money events if so.
    pendingMoneyDueToLateSessionState
      .get()
      .iterator()
      .asScala
      .toIterable
      .foreach(moneyEnrichmentBuilder => out.collect(moneyEnrichmentBuilder.withSessionInfo(Option(sessionInfo))))

    pendingMoneyDueToLateSessionState.clear()
  }

  override def handleLateMoneyEvent(
                                     money: MoneyEnrichmentBuilder,
                                     ctx: KeyedCoProcessFunction[Option[UUID], MoneyEnrichmentBuilder, UserSessionCreatedEvent, MoneyEnrichmentBuilder]#Context,
                                     out: Collector[MoneyEnrichmentBuilder]
  ): Unit =
    ctx.output(lateMoneyOutputTag, money)

  override def handleBufferedMoneyEvent(
                                         money: MoneyEnrichmentBuilder,
                                         ctx: KeyedCoProcessFunction[Option[UUID], MoneyEnrichmentBuilder, UserSessionCreatedEvent, MoneyEnrichmentBuilder]#OnTimerContext,
                                         out: Collector[MoneyEnrichmentBuilder]
  ): Unit = {
    if (money.money.isOldMoneyEvent) {
      ctx.output(lateMoneyOutputTag, money)
    } else {
      val sessionInfo = getSessionInfo
      if (sessionInfo.isDefined || money.money.userSessionId.isEmpty) {
        out.collect(money.withSessionInfo(sessionInfo))
      } else {
        pendingMoneyDueToLateSessionState.add(money)
      }
    }
  }

  override def handleOtherTimers(
                                  timestamp: Long,
                                  ctx: KeyedCoProcessFunction[Option[UUID], MoneyEnrichmentBuilder, UserSessionCreatedEvent, MoneyEnrichmentBuilder]#OnTimerContext,
                                  out: Collector[MoneyEnrichmentBuilder]
  ): Unit =
    handleSessionExpired(timestamp, ctx)

  private def handleSessionExpired(
      timerTimestamp: Long,
      ctx: KeyedCoProcessFunction[Option[UUID], MoneyEnrichmentBuilder, UserSessionCreatedEvent, MoneyEnrichmentBuilder]#OnTimerContext
  ): Unit = {
    logger.debug(
      s"Cleaning up state for sessionId [${ctx.getCurrentKey}] at [$timerTimestamp] and currentWatermark [${ctx.timerService().currentWatermark()}]"
    )
    sessionInfoState.clear()
  }

  private def getSessionInfo: Option[SessionInfo] = Option(sessionInfoState.value())
}

object MoneyMeetsSessionFunction {
  val SessionInfoQN = "money-session-join::session-info::query"
  val SessionInfoDesc = "money-session-join::session-info::desc"

  val PendingMoneyQN = "money-session-join::pending-money::query"
  val PendingMoneyDesc = "money-session-join::pending-money::desc"
}
