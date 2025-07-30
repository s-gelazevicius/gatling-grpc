package com.github.phisgr.gatling.grpc.check

import com.github.phisgr.gatling.grpc.check.GrpcCheck.Scope
//import com.softwaremill.quicklens._
import io.gatling.commons.validation.Validation
import io.gatling.core.check.{Check, CheckResult}
import io.gatling.core.session.{Expression, Session}

import java.util.{Map => JMap}
import scala.annotation.unchecked.uncheckedVariance

case class StreamCheck[-T](
  wrapped: Check[T@uncheckedVariance], scope: Scope
) extends CheckWithSelfType[T, StreamCheck[T]] {

  override def check(response: T, session: Session, cache: JMap[Any, Any]): Validation[CheckResult] =
    wrapped.check(response, session, cache)

  override def checkIf(condition: Expression[Boolean]): StreamCheck[T] =
    copy(wrapped = wrapped.checkIf(condition))

  override def checkIf(condition: (T @uncheckedVariance, Session) => Validation[Boolean]): StreamCheck[T] =
    copy(
      wrapped = wrapped.checkIf(condition),
      scope = if (scope.checksValue) scope else GrpcCheck.Close
    )

}
