package org.kindofdev.udfs

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.kindofdev.data.MoneyEnrichmentBuilder

class MoneyFirstOccurrenceFunction extends RichMapFunction[MoneyEnrichmentBuilder, MoneyEnrichmentBuilder] {

  private lazy val firstOccurrenceState: MapState[String, Boolean] = getRuntimeContext.getMapState(
    new MapStateDescriptor[String, Boolean]("first-occurrence", classOf[String], classOf[Boolean])
  )

  override def map(value: MoneyEnrichmentBuilder): MoneyEnrichmentBuilder = {
    val moneyTypeKey = value.money.moneyType
    val seen = firstOccurrenceState.contains(moneyTypeKey)
    firstOccurrenceState.put(moneyTypeKey, true)

    value.withFirstOccurrence(!seen)
  }
}
