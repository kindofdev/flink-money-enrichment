package org.kindofdev.udfs

import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeutils.base.LongSerializer
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.util.Collector
import org.kindofdev.Timestamp
import org.kindofdev.data._

import java.lang
import java.util.UUID

class MoneyMeetsUserFunction extends MoneyMeetsOtherAbstractFunction[UUID, UserEvent] {

  import MoneyMeetsUserFunction._

  private val userInfoDesc = new ValueStateDescriptor[UserInfo](UserInfoDesc, UserInfo.serializer)
  userInfoDesc.setQueryable(UserInfoQN)
  private lazy val userInfoState: ValueState[UserInfo] = getRuntimeContext.getState(userInfoDesc)

  private val userInfoBufferedDesc = new MapStateDescriptor[Timestamp, UserInfo](
    UserInfoBufferedDesc,
    LongSerializer.INSTANCE,
    UserInfo.serializer
  )
  userInfoBufferedDesc.setQueryable(UserInfoBufferedQN)
  private lazy val userInfoBuffered: MapState[lang.Long, UserInfo] = getRuntimeContext.getMapState(userInfoBufferedDesc)

  override def processElement2(
      value: UserEvent,
      ctx: KeyedCoProcessFunction[UUID, MoneyEnrichmentBuilder, UserEvent, MoneyEnrichmentBuilder]#Context,
      out: Collector[MoneyEnrichmentBuilder]
  ): Unit = {
    collectUserEvent(value).foreach { partialUserInfo =>
      userInfoBuffered.put(ctx.timestamp(), partialUserInfo)
      ctx.timerService().registerEventTimeTimer(ctx.timestamp())
    }
  }

  override def handleLateMoneyEvent(
      money: MoneyEnrichmentBuilder,
      ctx: KeyedCoProcessFunction[UUID, MoneyEnrichmentBuilder, UserEvent, MoneyEnrichmentBuilder]#Context,
      out: Collector[MoneyEnrichmentBuilder]
  ): Unit = handleMoney(money, out)

  override def handleBufferedMoneyEvent(
      money: MoneyEnrichmentBuilder,
      ctx: KeyedCoProcessFunction[UUID, MoneyEnrichmentBuilder, UserEvent, MoneyEnrichmentBuilder]#OnTimerContext,
      out: Collector[MoneyEnrichmentBuilder]
  ): Unit = handleMoney(money, out)

  override def handleOtherTimers(
      timestamp: Long,
      ctx: KeyedCoProcessFunction[UUID, MoneyEnrichmentBuilder, UserEvent, MoneyEnrichmentBuilder]#OnTimerContext,
      out: Collector[MoneyEnrichmentBuilder]
  ): Unit = {
    Option(userInfoBuffered.get(timestamp)) match {
      case Some(partialUserInfo) =>
        handleUserInfoBuffered(partialUserInfo)
        userInfoBuffered.remove(timestamp)
      case None =>
        throw new IllegalStateException(s"unexpected timer: [$timestamp]")
    }
  }

  private def handleMoney(event: MoneyEnrichmentBuilder, out: Collector[MoneyEnrichmentBuilder]): Unit = {
    val userInfo = getUserInfoState
    out.collect(event.withUserInfo(userInfo))
  }

  private def collectUserEvent(event: UserEvent): Option[UserInfo] = {
    event match {
      case ChangeBirthDateEvent(_, _, _, birthYear, _)            => Option(UserInfo(Option(birthYear), None))
      case ChangeAddressEvent(_, _, _, _, residentCountryCode, _) => Option(UserInfo(None, Option(residentCountryCode)))
      case _                                                      => None // Ignore
    }
  }

  private def handleUserInfoBuffered(userInfo: UserInfo): Unit = {
    val currentUserInfo = getUserInfoState.getOrElse(UserInfo.empty)
    userInfoState.update(currentUserInfo.combine(userInfo))
  }

  private def getUserInfoState: Option[UserInfo] = Option(userInfoState.value())

}

object MoneyMeetsUserFunction {
  val UserInfoQN = "money-user-join::user-info::query"
  val UserInfoDesc = "money-user-join::user-info::desc"

  val UserInfoBufferedQN = "money-user-join::user-info-buffered::query"
  val UserInfoBufferedDesc = "money-user-join::user-info-buffered::desc"
}
