package com.github.phisgr.gatling.grpc.action

import com.github.phisgr.gatling.forToMatch
import com.github.phisgr.gatling.generic.util.EventLoopHelper
import com.github.phisgr.gatling.grpc.ClientCalls
import com.github.phisgr.gatling.grpc.check.GrpcResponse
import com.github.phisgr.gatling.grpc.protocol.Statuses.{MultipleResponses, NoResponses}
import com.github.phisgr.gatling.grpc.request.UnaryResponse
import com.github.phisgr.gatling.grpc.util.{delayedParsing, statusCodeOption}
import io.gatling.commons.stats.{KO, OK}
import io.gatling.commons.util.Clock
import io.gatling.commons.validation.Validation
import io.gatling.core.Predef.value2Expression
import io.gatling.core.action.Action
import io.gatling.core.check.Check
import io.gatling.core.session.{Expression, Session}
import io.gatling.core.stats.StatsEngine
import io.gatling.core.structure.ScenarioContext
import io.gatling.core.controller.throttle.Throttler
import io.grpc._

class GrpcCallAction[Req, Res](
  builder: GrpcCallActionBuilder[Req, Res],
  ctx: ScenarioContext,
  override val next: Action
) extends UnaryResponse[Req, Res](builder, ctx) {
  override def loggingClass: Class[_] = getClass

  private[this] val throttler = ctx.coreComponents.throttler match {
    // not calling .filter to not make ctx a field
    case Some(throttler) if ctx.throttled => throttler
    case _ => null
  }

  override val clock: Clock = ctx.coreComponents.clock
  override val statsEngine: StatsEngine = ctx.coreComponents.statsEngine

  override val name: String = genName("grpcCall")
  override val requestName: Expression[String] = builder.requestName
  private[this] val payload = builder.payload
  private[this] val isSilent = builder.isSilent

  private def run(
    call: ClientCall[Req, Any],
    payload: Req,
    session: Session,
    resolvedRequestName: String,
    headers: Metadata
  ): Unit = {
    ClientCalls.asyncUnaryRequestCall(
      call, headers, payload,
      new ContinuingListener(session, resolvedRequestName, clock.nowMillis, headers, payload),
      streamingResponse = false
    )
  }

  override def sendRequest(session: Session): Validation[Unit] = forToMatch {
    for {
      name <- requestName(session)
      headers <- resolveHeaders(session)
      resolvedPayload <- payload(session)
      callOptions <- callOptions(session)
    } yield {
      val call = newCall(session, callOptions)
      if (throttler ne null) {
        throttler ! Throttler.Command.ThrottledRequest(
          session.scenario,
          () => run(call, resolvedPayload, session, resolvedRequestName = name, headers)
        )
      } else {
        run(call, resolvedPayload, session, resolvedRequestName = name, headers)
      }
    }
  }

  /**
   * After the call ends, [[onClose]] will be called,
   * then the execution will continue at [[run]] in the session event loop
   * and finally forward to the next action.
   *
   * See [[io.grpc.stub.ClientCalls.UnaryStreamToFuture]]
   *
   * The headers object is read for logging after ClientCall.start. This is supposedly not safe.
   * We are accessing it after the call closed, so let's hope nothing bad happens.
   *
   * @param session         the virtual user
   * @param fullRequestName the resolved request name
   * @param startTimestamp  start of the call
   * @param headers         for logging
   * @param payload         for logging
   */
  class ContinuingListener(
    session: Session,
    fullRequestName: String,
    startTimestamp: Long,
    headers: Metadata,
    payload: Req
  ) extends ClientCall.Listener[Any] with Runnable {
    // null if failed;
    // Res if we need the value in checks or trace logging;
    // Array[Byte] if we may need logging; Unit if neither
    private[this] var body: Any = _

    private[this] var grpcStatus: Status = _
    private[this] var trailers: Metadata = _
    private[this] var endTimestamp = 0L

    override def onHeaders(headers: Metadata): Unit = {}

    override def onMessage(message: Any): Unit = {
      if (null != body) {
        throw MultipleResponses
      }
      this.body = message
    }

    override def onClose(status: Status, trailers: Metadata): Unit = {
      endTimestamp = clock.nowMillis
      this.trailers = trailers
      grpcStatus = if (status.isOk && null == body) NoResponses else status

      // run() in session event loop
      session.eventLoop.checkAndExecute(this)
    }

    override def run(): Unit = {
      val (checkSaveUpdated, checkError) = Check.check(
        new GrpcResponse(body, grpcStatus, trailers),
        session,
        resolvedChecks,
        // Not using preparedCache because the prepare step is cheap
        preparedCache = null
      )

      val status = if (checkError.isEmpty) OK else KO
      val errorMessage = checkError.map(_.message)

      val newSession = if (isSilent) checkSaveUpdated else {
        val withStatus = if (status == KO) checkSaveUpdated.markAsFailed else checkSaveUpdated
        statsEngine.logResponse(
          withStatus.scenario,
          withStatus.groups,
          fullRequestName,
          startTimestamp = startTimestamp,
          endTimestamp = endTimestamp,
          status = status,
          responseCode = statusCodeOption(grpcStatus.getCode.value()),
          message = errorMessage
        )
        withStatus.logGroupRequestTimings(startTimestamp = startTimestamp, endTimestamp = endTimestamp)
      }

      def dump: String = {
        val bodyParsed = delayedParsing(body, responseMarshaller)

        def sessionToString(session: Session): String =
          session.attributes
            .map { case (k, v) => s"$k -> $v" }
            .mkString("Session:\n", "\n", "")

        def payloadToString(payload: Any): String =
          if (payload == null) "Payload: null" else payload.toString

        def responseToString(response: Any, grpcStatus: io.grpc.Status, trailers: io.grpc.Metadata): String = {
          val statusStr = if (grpcStatus == null) "Status: null" else s"Status: ${grpcStatus.getCode} - ${grpcStatus.getDescription}"
          val trailersStr = if (trailers == null) "Trailers: null" else {
            import scala.jdk.CollectionConverters._
            val entries = trailers.keys().asScala.map { key =>
              val value = trailers.get(io.grpc.Metadata.Key.of(key, io.grpc.Metadata.ASCII_STRING_MARSHALLER))
              s"$key -> $value"
            }
            entries.mkString("Trailers:\n", "\n", "")
          }
          s"Response:\n$response\n$statusStr\n$trailersStr"
        }

        val sb = new StringBuilder()

        sb.append(System.lineSeparator())
        sb.append(">>>>>>>>>>>>>>>>>>>>>>>>>>").append(System.lineSeparator())
        sb.append("Request:").append(System.lineSeparator())
        sb.append(s"$fullRequestName: $status ${errorMessage.getOrElse("")}").append(System.lineSeparator())
        sb.append("=========================").append(System.lineSeparator())
        sb.append(sessionToString(session)).append(System.lineSeparator())
        sb.append("=========================").append(System.lineSeparator())
        sb.append("gRPC request:").append(System.lineSeparator())
        sb.append(payloadToString(payload)).append(System.lineSeparator())
        sb.append(headers.toString).append(System.lineSeparator())
        sb.append("=========================").append(System.lineSeparator())
        sb.append("gRPC response:").append(System.lineSeparator())
        sb.append(responseToString(bodyParsed, grpcStatus, trailers)).append(System.lineSeparator())
        sb.append("<<<<<<<<<<<<<<<<<<<<<<<<<")

        sb.toString
      }

      if (status == KO) {
        logger.debug(s"Request '$fullRequestName' failed for user ${session.userId}: ${errorMessage.getOrElse("")}")
        if (!logger.underlying.isTraceEnabled) {
          logger.debug(dump)
        }
      }
      logger.trace(dump)

      next ! newSession
    }
  }

}
