package org.kindofdev.util

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.kindofdev.data.MoneyEnriched

import java.util.Collections

class CollectSink extends SinkFunction[MoneyEnriched] {
  override def invoke(value: MoneyEnriched, context: SinkFunction.Context): Unit = CollectSink.values.add(value)
}

object CollectSink {
  val values: java.util.List[MoneyEnriched] = Collections.synchronizedList(new java.util.ArrayList())
}
