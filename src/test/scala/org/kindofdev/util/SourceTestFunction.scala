package org.kindofdev.util

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.kindofdev.data.Event
import org.kindofdev.util.TestIORepo.WatermarkOrEvent

class SourceTestFunction[T <: Event](events: Seq[WatermarkOrEvent[T]]) extends SourceFunction[T] {

  override def run(ctx: SourceFunction.SourceContext[T]): Unit = {
    for (elem <- events) {
      elem match {
        case Left(watermark) => ctx.emitWatermark(new Watermark(watermark.timestamp))
        case Right(event)    => ctx.collectWithTimestamp(event, event.processedAt.toEpochMilli)
      }
    }
  }

  override def cancel(): Unit = {}
}
