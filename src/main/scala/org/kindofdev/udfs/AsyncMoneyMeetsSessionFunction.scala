package org.kindofdev.udfs

import cats.data.EitherT
import org.apache.flink.runtime.concurrent.Executors.directExecutor
import org.apache.flink.streaming.api.scala.async.{AsyncFunction, ResultFuture}
import org.kindofdev.data.MoneyEnrichmentBuilder
import org.kindofdev.io.SessionHttpClient
import org.kindofdev.log.Logging
import org.kindofdev.udfs.AsyncMoneyMeetsSessionFunction.MoneyWithSessionAsyncError

import scala.concurrent.ExecutionContext

class AsyncMoneyMeetsSessionFunction(sessionHttpClient: SessionHttpClient)
    extends AsyncFunction[MoneyEnrichmentBuilder, MoneyEnrichmentBuilder]
    with Logging {

  implicit lazy val ec: ExecutionContext = ExecutionContext.fromExecutor(directExecutor())

  override def asyncInvoke(input: MoneyEnrichmentBuilder, resultFuture: ResultFuture[MoneyEnrichmentBuilder]): Unit = {
    logger.debug("Async enriching with session - money id: [{}]", input.money.id)

    input.money.userSessionId match {
      case None =>
        resultFuture.completeExceptionally(MoneyWithSessionAsyncError)
      case Some(userSessionId) =>
        (for {
          sessionInfo <- EitherT(sessionHttpClient.getSessionInfo(userSessionId).recover { case thr => Left(thr) })
        } yield {
          resultFuture.complete(Iterable(input.withSessionInfo(Option(sessionInfo))))
        }).leftMap(thr => resultFuture.completeExceptionally(thr))
    }
  }

}

object AsyncMoneyMeetsSessionFunction {
  case object MoneyWithSessionAsyncError extends Throwable
}
