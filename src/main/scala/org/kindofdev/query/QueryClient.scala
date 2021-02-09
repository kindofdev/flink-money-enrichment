package org.kindofdev.query

import cats.implicits._
import org.apache.flink.api.common.state.{ListStateDescriptor, ValueStateDescriptor}
import org.apache.flink.api.common.{ExecutionConfig, JobID}
import org.apache.flink.api.scala._
import org.apache.flink.queryablestate.client.QueryableStateClient
import org.kindofdev.data.{MoneyEnrichmentBuilder, SessionInfo, UserInfo}
import org.kindofdev.udfs.MoneyMeetsSessionFunction.{PendingMoneyDesc, SessionInfoDesc}
import org.kindofdev.udfs.MoneyMeetsUserFunction.UserInfoDesc
import org.kindofdev.udfs.{MoneyMeetsSessionFunction, MoneyMeetsUserFunction}

import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._

class QueryClient(jobId: JobID, hostname: String = "127.0.1.1", port: Int = 9069) {

  private val client = new QueryableStateClient(hostname, port)
  client.setExecutionConfig(new ExecutionConfig)

  private val sessionInfoDesc = new ValueStateDescriptor[SessionInfo](SessionInfoDesc, SessionInfo.serializer)

  private val pendingMoneyDueToLateSessionDesc =
    new ListStateDescriptor[MoneyEnrichmentBuilder](PendingMoneyDesc, MoneyEnrichmentBuilder.serializer)

  private val userInfoDesc = new ValueStateDescriptor[UserInfo](UserInfoDesc, UserInfo.serializer)

  def executeQuery(queryableStateName: String, key: UUID): Unit = {

    queryableStateName match {
      case MoneyMeetsSessionFunction.SessionInfoQN =>
        val state = client
          .getKvState(jobId, queryableStateName, key.some, createTypeInformation[Option[UUID]], sessionInfoDesc)
          .get(10, TimeUnit.SECONDS)

        printState(jobId, queryableStateName, key, state.value().toString)

      case MoneyMeetsSessionFunction.PendingMoneyQN =>
        val state = client
          .getKvState(jobId, queryableStateName, key.some, createTypeInformation[Option[UUID]], pendingMoneyDueToLateSessionDesc)
          .get(10, TimeUnit.SECONDS)

        printState(jobId, queryableStateName, key, state.get().asScala.map(_.money.id).mkString("\n"))

      case MoneyMeetsUserFunction.UserInfoQN =>
        val state = client.getKvState(jobId, queryableStateName, key, createTypeInformation[UUID], userInfoDesc).get(10, TimeUnit.SECONDS)
        printState(jobId, queryableStateName, key, state.value().toString)

      case _ => throw new IllegalArgumentException("Invalid queryableStateName")
    }

    client.shutdownAndWait()
  }

  private def printState(jobId: JobID, queryableStateName: String, key: UUID, stateFormatted: String): Unit = {
    println("\n")
    println(s"JobId: [$jobId]")
    println(s"QueryableStateName: [$jobId]")
    println(s"Key: [$key]")
    println("\n")
    println(s"----------------------------------STATE----------------------------------\n")
    println(stateFormatted)
    println("\n")
    println(s"-------------------------------------------------------------------------")
  }

}
