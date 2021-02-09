package org.kindofdev

import com.typesafe.config.Config
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.OutputTag
import org.kindofdev.data.MoneyEnrichmentBuilder

import java.time.Duration
import java.util.Properties

class Settings(config: Config) {

  val walletTopic = "wallet-events"
  val userTopic = "user-events"
  val userSessionTopic = "user-session-events"
  val moneyEnrichedTopic = "money-enriched-events"

  val lateMoneyPrintSink = "late-money-sink"
  val lateBusinessMoneyPrintSink = "late-business-money-sink"
  val moneyEnrichedPrintSink = "money-enriched-sink"

  val dedupMoneyTTL: Duration = config.getDuration("dedupMoneyTTL")
  val sessionTTL: Duration = config.getDuration("sessionTTL")

  val lateMoneyOutputTag: OutputTag[MoneyEnrichmentBuilder] = OutputTag[MoneyEnrichmentBuilder]("late-money-tag")

  val asyncSessionRequestCapacity: Int = config.getInt("asyncSessionRequestCapacity")
  val asyncSessionRequestTimeoutInMillis: Long = config.getLong("asyncSessionRequestTimeoutInMillis")

  val checkpointURI: String = config.getString("checkpointURI")

  val defaultParallelism: Int = config.getInt("defaultParallelism")
  val checkpointingIntervalInMillis: Long = config.getLong("checkpointingIntervalInMillis")

  def getKafkaProperties: Properties = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "money-enrichment")
    properties
  }

}
