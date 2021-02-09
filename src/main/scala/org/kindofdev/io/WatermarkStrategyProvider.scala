package org.kindofdev.io

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.kindofdev.data.{UserEvent, UserSessionEvent, WalletEvent}

import java.time.Duration

object WatermarkStrategyProvider {

  val outOfOrderForWalletEvents: WatermarkStrategy[WalletEvent] =
    WatermarkStrategy
      .forBoundedOutOfOrderness(Duration.ofSeconds(20))
      .withIdleness(Duration.ofMillis(1))
      .withTimestampAssigner(new SerializableTimestampAssigner[WalletEvent] {
        override def extractTimestamp(element: WalletEvent, recordTimestamp: Long): Long = element.processedAt.toEpochMilli
      })

  val outOfOrderForUserEvents: WatermarkStrategy[UserEvent] =
    WatermarkStrategy
      .forBoundedOutOfOrderness(Duration.ofSeconds(20))
      .withIdleness(Duration.ofMillis(1))
      .withTimestampAssigner(new SerializableTimestampAssigner[UserEvent] {
        override def extractTimestamp(element: UserEvent, recordTimestamp: Long): Long = element.processedAt.toEpochMilli
      })

  val outOfOrderForUserSessionEvents: WatermarkStrategy[UserSessionEvent] =
    WatermarkStrategy
      .forBoundedOutOfOrderness(Duration.ofSeconds(20))
      .withIdleness(Duration.ofMillis(1))
      .withTimestampAssigner(new SerializableTimestampAssigner[UserSessionEvent] {
        override def extractTimestamp(element: UserSessionEvent, recordTimestamp: Long): Long = element.processedAt.toEpochMilli
      })
}
