package org.kindofdev.util

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.kindofdev.data._
import org.kindofdev.io.IORepo
import org.kindofdev.util.TestIORepo.Events

class TestIORepo(events: Events, sink: SinkFunction[MoneyEnriched]) extends IORepo {

  override def walletEventSourceFunction: SourceFunction[WalletEvent] =
    new SourceTestFunction[WalletEvent](events.walletEvents)

  override def userEventSourceFunction: SourceFunction[UserEvent] =
    new SourceTestFunction[UserEvent](events.userEvents)

  override def userSessionEventSourceFunction: SourceFunction[UserSessionEvent] =
    new SourceTestFunction[UserSessionEvent](events.sessionEvents)

  override def moneyEnrichedSinkFunction: SinkFunction[MoneyEnriched] = sink

}

object TestIORepo {

  case class Watermark(timestamp: Long) // flink Watermark is not serializable

  type WatermarkOrEvent[T <: Event] = Either[Watermark, T]

  case class Events(
                     sessionEvents: Seq[WatermarkOrEvent[UserSessionEvent]],
                     userEvents: Seq[WatermarkOrEvent[UserEvent]],
                     walletEvents: Seq[WatermarkOrEvent[WalletEvent]]
  )
}
