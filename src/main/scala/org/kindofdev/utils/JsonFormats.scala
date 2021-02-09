package org.kindofdev.utils

import com.vitorsvieira.iso.ISOCurrency
import com.vitorsvieira.iso.ISOCurrency.ISOCurrency
import play.api.libs.json.{JsValue, JsonValidationError, _}
import play.api.libs.functional.syntax._

import java.util.UUID
import scala.util.Try

object JsonFormats {

  implicit val isoCurrencyFormat: Format[ISOCurrency] = new Format[ISOCurrency] {

    def reads(json: JsValue): JsResult[ISOCurrency] = json match {
      case JsString(s) => Try(ISOCurrency(s)).map(JsSuccess(_)).getOrElse(JsError(s"currency is incorrect: $s"))
      case _           => JsError("String value expected")
    }
    def writes(currency: ISOCurrency): JsString = JsString(currency.toString())
  }

  implicit val uuidReads: Reads[UUID] = implicitly[Reads[String]]
    .collect(JsonValidationError("Invalid UUID"))(Function.unlift { str =>
      Try(UUID.fromString(str)).toOption
    })

  implicit val uuidWrites: Writes[UUID] = Writes { uuid =>
    JsString(uuid.toString)
  }

}
