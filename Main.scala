package momijikawa.exercisezmq

import scala.concurrent.ExecutionContext

object Main extends App {
  import akka.actor._
  import akka.zeromq._

  class RepActor extends Actor with ActorLogging {
    import log.{ debug, warning }

    def receive = {
      case m: ZMQMessage =>
        debug(s"message inbound: ${m.frames}")
        sender ! ZMQMessage(m.frame(0), m.frame(1).mapI(_ + 1))

      case unknown => warning("Unknown: $unknown")
    }
  }

  override def main(args: Array[String]): Unit = {
    val system = ActorSystem("Exercise-ZMQ-Server")
    implicit def execContext: ExecutionContext = system.dispatcher
    val listener = system.actorOf(Props[RepActor], "repActor")

    val repSocket = ZeroMQExtension(system).newSocket(SocketType.Rep,
      Listener(listener),
      Bind("tcp://127.0.0.1:21231"))

    println("test start on\ntcp://127.0.0.1:21231")
  }
}