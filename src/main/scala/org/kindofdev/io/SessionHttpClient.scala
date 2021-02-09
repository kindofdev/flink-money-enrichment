package org.kindofdev.io

import org.kindofdev.data.SessionInfo
import org.kindofdev.log.Logging

import java.util.UUID
import scala.concurrent.Future
import scala.concurrent.duration.Duration

trait SessionHttpClient {
  def getSessionInfo(userSessionId: UUID): Future[Either[Throwable, SessionInfo]]
}

object SessionHttpClient {

  class SessionHttpClientFake(countryCode: String, responseTime: Duration) extends SessionHttpClient with Serializable with Logging {
    override def getSessionInfo(userSessionId: UUID): Future[Either[Throwable, SessionInfo]] = {
      Future.successful {
        Thread.sleep(responseTime.toMillis)
        Right(SessionInfo(clientCountryCode = countryCode))
      }
    }
  }
}
