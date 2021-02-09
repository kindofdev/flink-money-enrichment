package org.kindofdev.io

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.kindofdev.Settings
import org.kindofdev.data._
import org.kindofdev.data.serde.{MoneyEnrichedSerializationSchema, UserDeserializationSchema, UserSessionDeserializationSchema, WalletDeserializationSchema}

trait IORepo {
  def walletEventSourceFunction: SourceFunction[WalletEvent]
  def userEventSourceFunction: SourceFunction[UserEvent]
  def userSessionEventSourceFunction: SourceFunction[UserSessionEvent]
  def moneyEnrichedSinkFunction: SinkFunction[MoneyEnriched]
}

object IORepo {

  class LiveIORepo(config: Settings) extends IORepo {

    override def walletEventSourceFunction: SourceFunction[WalletEvent] = {
      val kafkaConsumer =
        new FlinkKafkaConsumer[WalletEvent](config.walletTopic, new WalletDeserializationSchema, config.getKafkaProperties)
      kafkaConsumer.setStartFromEarliest()
      kafkaConsumer.assignTimestampsAndWatermarks(WatermarkStrategyProvider.outOfOrderForWalletEvents)
    }

    override def userEventSourceFunction: SourceFunction[UserEvent] = {
      val kafkaConsumer =
        new FlinkKafkaConsumer[UserEvent](config.userTopic, new UserDeserializationSchema, config.getKafkaProperties)
      kafkaConsumer.setStartFromEarliest()
      kafkaConsumer.assignTimestampsAndWatermarks(WatermarkStrategyProvider.outOfOrderForUserEvents)

    }

    override def userSessionEventSourceFunction: SourceFunction[UserSessionEvent] = {
      val kafkaConsumer = new FlinkKafkaConsumer[UserSessionEvent](
        config.userSessionTopic,
        new UserSessionDeserializationSchema,
        config.getKafkaProperties
      )
      kafkaConsumer.setStartFromEarliest()
      kafkaConsumer.assignTimestampsAndWatermarks(WatermarkStrategyProvider.outOfOrderForUserSessionEvents)
    }

    override def moneyEnrichedSinkFunction: SinkFunction[MoneyEnriched] =
      new FlinkKafkaProducer[MoneyEnriched](config.moneyEnrichedTopic, new MoneyEnrichedSerializationSchema, config.getKafkaProperties)

  }

}
