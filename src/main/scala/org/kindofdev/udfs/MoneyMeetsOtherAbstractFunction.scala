package org.kindofdev.udfs

import org.apache.flink.api.common.state._
import org.apache.flink.api.common.typeutils.base.LongSerializer
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.util.Collector
import org.kindofdev.Timestamp
import org.kindofdev.data._
import org.kindofdev.log.Logging

import java.lang

abstract class MoneyMeetsOtherAbstractFunction[K, T]
    extends KeyedCoProcessFunction[
      K,
      MoneyEnrichmentBuilder,
      T,
      MoneyEnrichmentBuilder
    ]
    with Logging {

  import MoneyMeetsOtherAbstractFunction._

  protected val moneyBufferedDesc = new MapStateDescriptor[Timestamp, MoneyEnrichmentBuilder](
    MoneyBufferedDesc,
    LongSerializer.INSTANCE,
    MoneyEnrichmentBuilder.serializer
  )
  protected lazy val moneyBuffered: MapState[lang.Long, MoneyEnrichmentBuilder] = getRuntimeContext.getMapState(moneyBufferedDesc)

  override def processElement1(
      value: MoneyEnrichmentBuilder,
      ctx: KeyedCoProcessFunction[K, MoneyEnrichmentBuilder, T, MoneyEnrichmentBuilder]#Context,
      out: Collector[MoneyEnrichmentBuilder]
  ): Unit = {
    val currentWatermark = ctx.timerService().currentWatermark()
    val eventTimestamp = ctx.timestamp()
    if (eventTimestamp < currentWatermark) {
      handleLateMoneyEvent(value, ctx, out)
    } else {
      moneyBuffered.put(ctx.timestamp(), value)
      ctx.timerService().registerEventTimeTimer(ctx.timestamp())
    }
  }

  override def onTimer(
      timestamp: Long,
      ctx: KeyedCoProcessFunction[K, MoneyEnrichmentBuilder, T, MoneyEnrichmentBuilder]#OnTimerContext,
      out: Collector[MoneyEnrichmentBuilder]
  ): Unit = {
    Option(moneyBuffered.get(timestamp)) match {
      case Some(money) =>
        handleBufferedMoneyEvent(money, ctx, out)
        moneyBuffered.remove(timestamp)
      case None =>
        handleOtherTimers(timestamp, ctx, out)
    }
  }

  def handleOtherTimers(
      timestamp: Long,
      ctx: KeyedCoProcessFunction[K, MoneyEnrichmentBuilder, T, MoneyEnrichmentBuilder]#OnTimerContext,
      out: Collector[MoneyEnrichmentBuilder]
  ): Unit

  case class TimerContext(
      timestamp: Long,
      ctx: KeyedCoProcessFunction[K, MoneyEnrichmentBuilder, T, MoneyEnrichmentBuilder]#OnTimerContext,
      out: Collector[MoneyEnrichmentBuilder]
  )

  def handleLateMoneyEvent(
      money: MoneyEnrichmentBuilder,
      ctx: KeyedCoProcessFunction[K, MoneyEnrichmentBuilder, T, MoneyEnrichmentBuilder]#Context,
      out: Collector[MoneyEnrichmentBuilder]
  ): Unit

  def handleBufferedMoneyEvent(
      money: MoneyEnrichmentBuilder,
      ctx: KeyedCoProcessFunction[K, MoneyEnrichmentBuilder, T, MoneyEnrichmentBuilder]#OnTimerContext,
      out: Collector[MoneyEnrichmentBuilder]
  ): Unit

}

object MoneyMeetsOtherAbstractFunction {
  val MoneyBufferedDesc = "money-buffered::desc"
}
