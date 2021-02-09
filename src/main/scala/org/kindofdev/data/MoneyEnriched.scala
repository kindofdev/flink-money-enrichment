package org.kindofdev.data

import play.api.libs.json.{Format, Json}

case class MoneyEnriched(money: Money, userInfo: Option[UserInfo], sessionInfo: Option[SessionInfo], firstOccurrence: Boolean)

object MoneyEnriched {
  implicit val format: Format[MoneyEnriched] = Json.format[MoneyEnriched]
}
