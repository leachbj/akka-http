/**
 * Copyright (C) 2009-2016 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.http.impl.engine.client

import akka.NotUsed
import akka.http.scaladsl.settings.{ ClientConnectionSettings, ParserSettings }
import language.existentials
import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import akka.stream.io.{ SessionBytes, SslTlsInbound, SendBytes }
import akka.util.ByteString
import akka.event.LoggingAdapter
import akka.stream._
import akka.stream.scaladsl._
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.Host
import akka.http.scaladsl.model.{ IllegalResponseException, HttpMethod, HttpRequest, HttpResponse, ResponseEntity }
import akka.http.impl.engine.rendering.{ RequestRenderingContext, HttpRequestRendererFactory }
import akka.http.impl.engine.parsing._
import akka.http.impl.util._
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.{ InHandler, OutHandler }
import akka.stream.impl.fusing.SubSource

/**
 * INTERNAL API
 */
private[http] object OutgoingConnectionBlueprint {
  /*
    Stream Setup
    ============

    requestIn                                            +----------+
    +-----------------------------------------------+--->|  Termi-  |   requestRendering
                                                    |    |  nation  +---------------------> |
                 +-------------------------------------->|  Merge   |                       |
                 | Termination Backchannel          |    +----------+                       |  TCP-
                 |                                  |                                       |  level
                 |                                  | Method                                |  client
                 |                +------------+    | Bypass                                |  flow
    responseOut  |  responsePrep  |  Response  |<---+                                       |
    <------------+----------------|  Parsing   |                                            |
                                  |  Merge     |<------------------------------------------ V
                                  +------------+
  */
  def apply(hostHeader: Host,
            settings: ClientConnectionSettings,
            log: LoggingAdapter): Http.ClientLayer = {
    import settings._

    // the initial header parser we initially use for every connection,
    // will not be mutated, all "shared copy" parsers copy on first-write into the header cache
    val rootParser = new HttpResponseParser(parserSettings, HttpHeaderParser(parserSettings) { info ⇒
      if (parserSettings.illegalHeaderWarnings)
        logParsingError(info withSummaryPrepended "Illegal response header", log, parserSettings.errorLoggingVerbosity)
    })

    val requestRendererFactory = new HttpRequestRendererFactory(userAgentHeader, requestHeaderSizeHint, log)

    val requestRendering: Flow[HttpRequest, ByteString, NotUsed] = Flow[HttpRequest]
      .map(RequestRenderingContext(_, hostHeader))
      .via(Flow[RequestRenderingContext].flatMapConcat(requestRendererFactory.renderToSource).named("renderer"))

    val methodBypass = Flow[HttpRequest].map(_.method)

    import ParserOutput._
    val responsePrep = Flow[List[ResponseOutput]]
      .mapConcat(conforms)
      .via(new ResponsePrep(parserSettings))

    val core = BidiFlow.fromGraph(GraphDSL.create() { implicit b ⇒
      import GraphDSL.Implicits._
      val methodBypassFanout = b.add(Broadcast[HttpRequest](2, eagerCancel = true))
      val responseParsingMerge = b.add(new ResponseParsingMerge(rootParser))

      val terminationFanout = b.add(Broadcast[HttpResponse](2))
      val terminationMerge = b.add(TerminationMerge)

      val logger = b.add(Flow[ByteString].transform(() ⇒ errorHandling((t: Throwable) ⇒ log.error(t, "Outgoing request stream error"))).named("errorLogger"))
      val wrapTls = b.add(Flow[ByteString].map(SendBytes))
      terminationMerge.out ~> requestRendering ~> logger ~> wrapTls

      val collectSessionBytes = b.add(Flow[SslTlsInbound].collect { case s: SessionBytes ⇒ s })
      collectSessionBytes ~> responseParsingMerge.in0

      methodBypassFanout.out(0) ~> terminationMerge.in0

      methodBypassFanout.out(1) ~> methodBypass ~> responseParsingMerge.in1

      responseParsingMerge.out ~> responsePrep ~> terminationFanout.in
      terminationFanout.out(0) ~> terminationMerge.in1

      BidiShape(
        methodBypassFanout.in,
        wrapTls.out,
        collectSessionBytes.in,
        terminationFanout.out(1))
    })

    One2OneBidiFlow[HttpRequest, HttpResponse](-1) atop core
  }

  // a simple merge stage that simply forwards its first input and ignores its second input
  // (the terminationBackchannelInput), but applies a special completion handling
  private object TerminationMerge extends GraphStage[FanInShape2[HttpRequest, HttpResponse, HttpRequest]] {
    private val requests = Inlet[HttpRequest]("requests")
    private val responses = Inlet[HttpResponse]("responses")
    private val out = Outlet[HttpRequest]("out")

    override def initialAttributes = Attributes.name("TerminationMerge")

    val shape = new FanInShape2(requests, responses, out)

    override def createLogic(effectiveAttributes: Attributes) = new GraphStageLogic(shape) {
      passAlong(requests, out, doFinish = false, doFail = true)
      setHandler(out, eagerTerminateOutput)

      setHandler(responses, new InHandler {
        override def onPush(): Unit = pull(responses)
      })

      override def preStart(): Unit = {
        pull(requests)
        pull(responses)
      }
    }
  }

  import ParserOutput._

  private final class ResponsePrep(parserSettings: ParserSettings)
    extends GraphStage[FlowShape[ResponseOutput, HttpResponse]] {

    private val in = Inlet[ResponseOutput]("ResponsePrep.in")
    private val out = Outlet[HttpResponse]("ResponsePrep.out")

    val shape = new FlowShape(in, out)

    override def createLogic(effectiveAttributes: Attributes) = new GraphStageLogic(shape) with InHandler with OutHandler {
      private var entitySource: SubSourceOutlet[ResponseOutput] = _
      private def entitySubstreamStarted = entitySource ne null
      private def idle = this

      def setIdleHandlers(): Unit = {
        setHandler(in, idle)
        setHandler(out, idle)
      }

      def onPush(): Unit = grab(in) match {
        case ResponseStart(statusCode, protocol, headers, entityCreator, _) ⇒
          val entity = createEntity(entityCreator) withSizeLimit parserSettings.maxContentLength
          push(out, HttpResponse(statusCode, headers, entity, protocol))

        case MessageStartError(_, info) ⇒
          throw IllegalResponseException(info)

        case other ⇒
          throw new IllegalStateException(s"ResponseStart expected but $other received.")
      }

      def onPull(): Unit = {
        if (!entitySubstreamStarted) pull(in)
      }

      setIdleHandlers()

      private lazy val waitForMessageEnd = new InHandler {
        def onPush(): Unit = grab(in) match {
          case MessageEnd ⇒ setHandler(in, idle)
          case other      ⇒ throw new IllegalStateException(s"MessageEnd expected but $other received.")
        }
      }

      private lazy val substreamHandler = new InHandler with OutHandler {
        override def onPush(): Unit = grab(in) match {
          case MessageEnd ⇒
            entitySource.complete()
            entitySource = null
            setIdleHandlers()

          case messagePart ⇒
            entitySource.push(messagePart)
        }

        override def onPull(): Unit = pull(in)

        override def onUpstreamFinish(): Unit = {
          entitySource.complete()
          completeStage()
        }

        override def onUpstreamFailure(reason: Throwable): Unit = {
          entitySource.fail(reason)
          failStage(reason)
        }

        override def onDownstreamFinish(): Unit = {
          entitySource.complete()
          completeStage()
        }
      }

      private def createEntity(creator: EntityCreator[ResponseOutput, ResponseEntity]): ResponseEntity = {
        creator match {
          case StrictEntityCreator(entity) ⇒
            pull(in)
            setHandler(in, waitForMessageEnd)
            entity

          case StreamedEntityCreator(creator) ⇒
            entitySource = new SubSourceOutlet[ResponseOutput]("EntitySource")
            entitySource.setHandler(substreamHandler)
            setHandler(in, substreamHandler)
            creator(Source.fromGraph(entitySource.source))
        }
      }
    }
  }

  /**
   * A merge that follows this logic:
   * 1. Wait on the methodBypass for the method of the request corresponding to the next response to be received
   * 2. Read from the dataInput until exactly one response has been fully received
   * 3. Go back to 1.
   */
  class ResponseParsingMerge(rootParser: HttpResponseParser) extends GraphStage[FanInShape2[SessionBytes, HttpMethod, List[ResponseOutput]]] {
    private val dataInput = Inlet[SessionBytes]("data")
    private val methodBypassInput = Inlet[HttpMethod]("method")
    private val out = Outlet[List[ResponseOutput]]("out")

    override def initialAttributes = Attributes.name("ResponseParsingMerge")

    val shape = new FanInShape2(dataInput, methodBypassInput, out)

    override def createLogic(effectiveAttributes: Attributes) = new GraphStageLogic(shape) {
      // each connection uses a single (private) response parser instance for all its responses
      // which builds a cache of all header instances seen on that connection
      val parser = rootParser.createShallowCopy()
      var waitingForMethod = true

      setHandler(methodBypassInput, new InHandler {
        override def onPush(): Unit = {
          val method = grab(methodBypassInput)
          parser.setRequestMethodForNextResponse(method)
          val output = parser.parseBytes(ByteString.empty)
          drainParser(output)
        }
        override def onUpstreamFinish(): Unit =
          if (waitingForMethod) completeStage()
      })

      setHandler(dataInput, new InHandler {
        override def onPush(): Unit = {
          val bytes = grab(dataInput)
          val output = parser.parseSessionBytes(bytes)
          drainParser(output)
        }
        override def onUpstreamFinish(): Unit =
          if (waitingForMethod) completeStage()
          else {
            if (parser.onUpstreamFinish()) {
              completeStage()
            } else {
              emit(out, parser.onPull() :: Nil, () ⇒ completeStage())
            }
          }
      })

      setHandler(out, eagerTerminateOutput)

      val getNextMethod = () ⇒ {
        waitingForMethod = true
        if (isClosed(methodBypassInput)) completeStage()
        else pull(methodBypassInput)
      }

      val getNextData = () ⇒ {
        waitingForMethod = false
        if (isClosed(dataInput)) completeStage()
        else pull(dataInput)
      }

      @tailrec def drainParser(current: ResponseOutput, b: ListBuffer[ResponseOutput] = ListBuffer.empty): Unit = {
        def e(output: List[ResponseOutput], andThen: () ⇒ Unit): Unit =
          if (output.nonEmpty) emit(out, output, andThen)
          else andThen()
        current match {
          case NeedNextRequestMethod ⇒ e(b.result(), getNextMethod)
          case StreamEnd             ⇒ e(b.result(), () ⇒ completeStage())
          case NeedMoreData          ⇒ e(b.result(), getNextData)
          case x                     ⇒ drainParser(parser.onPull(), b += x)
        }
      }

      override def preStart(): Unit = getNextMethod()
    }
  }
}
