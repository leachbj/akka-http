/*
 * Copyright (C) 2009-2015 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.http.impl.engine.ws

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.ws.{ Message, UpgradeToWebsocket }
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Flow

object WSServerAutobahnTest extends App {
  implicit val system = ActorSystem("WSServerTest")
  implicit val fm = ActorMaterializer()

  val host = sys.env.getOrElse("akka.ws-host", "127.0.0.1")
  val port = sys.env.getOrElse("akka.ws-port", "9001").toInt
  val mode = sys.env.getOrElse("akka.ws-mode", "read") // read or sleep

  try {
    val binding = Http().bindAndHandleSync({
      case req @ HttpRequest(GET, Uri.Path("/"), _, _, _) if req.header[UpgradeToWebsocket].isDefined ⇒
        req.header[UpgradeToWebsocket] match {
          case Some(upgrade) ⇒ upgrade.handleMessages(echoWebsocketService) // needed for running the autobahn test suite
          case None          ⇒ HttpResponse(400, entity = "Not a valid websocket request!")
        }
      case _: HttpRequest ⇒ HttpResponse(404, entity = "Unknown resource!")
    },
      interface = host, // adapt to your docker host IP address if necessary
      port = port)

    Await.result(binding, 3.second) // throws if binding fails
    println(s"Server online at http://${host}:${port}")
    mode match {
      case "sleep" => while (true) Thread.sleep(1.minute.toMillis)
      case "read" => Console.readLine("Press RETURN to stop...")
      case _ => throw new Exception("akka.ws-mode MUST be sleep or read.")
    }
  } finally {
    system.shutdown()
  }

  def echoWebsocketService: Flow[Message, Message, Unit] =
    Flow[Message] // just let message flow directly to the output
}
