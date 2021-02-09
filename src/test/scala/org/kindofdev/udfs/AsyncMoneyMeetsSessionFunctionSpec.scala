package org.kindofdev.udfs

import org.apache.flink.streaming.api.scala.async.ResultFuture
import org.kindofdev.data.{DataGenerator, Money, MoneyEnrichmentBuilder, SessionInfo}
import org.kindofdev.io.SessionHttpClient
import org.kindofdev.udfs.AsyncMoneyMeetsSessionFunction.MoneyWithSessionAsyncError
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class AsyncMoneyMeetsSessionFunctionSpec extends AnyFlatSpec with Matchers with MockFactory {

  "AsyncMoneyMeetsSessionFunction" should "enrich money with session from an external service" in {
    val function = new AsyncMoneyMeetsSessionFunction(new SessionHttpClient.SessionHttpClientFake("UY", 100.millis))
    val resultFuture = mock[ResultFuture[MoneyEnrichmentBuilder]]

    val money = Money.fromMoneyEvent(DataGenerator.createTransactionEvent())
    val input = MoneyEnrichmentBuilder(money)

    val expected = input.withSessionInfo(Option(SessionInfo(clientCountryCode = "UY")))
    (resultFuture.complete _).expects(Iterable(expected))

    function.asyncInvoke(input, resultFuture)
  }

  "AsyncMoneyMeetsSessionFunction" should "fail if money doesn't have session" in {
    val function = new AsyncMoneyMeetsSessionFunction(new SessionHttpClient.SessionHttpClientFake("UY", 100.millis))
    val resultFuture = mock[ResultFuture[MoneyEnrichmentBuilder]]

    val money = Money.fromMoneyEvent(DataGenerator.createTransactionEvent(userSessionId = None))
    val input = MoneyEnrichmentBuilder(money)

    (resultFuture.completeExceptionally _).expects(MoneyWithSessionAsyncError)

    function.asyncInvoke(input, resultFuture)
  }

}
