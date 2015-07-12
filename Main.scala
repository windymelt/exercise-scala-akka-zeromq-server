package momijikawa.exercisezmq

import akka.actor.ActorRef
import akka.util.ByteString
import akka.zeromq.ZMQMessage
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}
import scala.collection.immutable.Seq

case class RxRaw(msg: ZMQMessage)

object Main extends App {
  import akka.actor._
  import akka.zeromq._
  import akka.pattern.ask

  class RepActor(upperLayer: ActorRef) extends Actor with ActorLogging {
    import log.{ debug, warning }

    def receive = {
      case m: ZMQMessage =>
        debug(s"message inbound: ${m.frames}")
        upperLayer ! RxRaw(m)

      case unknown => warning("Unknown: $unknown")
    }
  }

  class SessionLayer(listener: ActorRef) extends Actor with ActorLogging {
    import log.{debug, warning, error => logError}
    import scala.concurrent.duration._

    implicit val timeout: akka.util.Timeout = 10 seconds
    implicit val execContext: ExecutionContext = context.dispatcher

    val zmqListener = context.system.actorOf(Props(classOf[RepActor], self), "repActor")

    val repSocket = ZeroMQExtension(context.system).newSocket(SocketType.Rep,
      Listener(zmqListener),
      Bind("tcp://127.0.0.1:21231"))

    def receive = {
      case RxRaw(m) => forwardMessage(m, sender)
      case unknown => logError("Unknown message received: " + unknown)
    }

    /**
     * 返信メッセージにセッション番号を付与し送信させる。
     *
     * 受信メッセージからセッション番号を取り出し、返信アドレスマップ中の対応するアクターに
     * セッション番号を取り除いたメッセージを送信する。
     */
    def sendMessage(xs: Seq[ByteString], sessionNumber: Int, sender: ActorRef): Unit = {
      val attachedMessage: Seq[ByteString] = ByteString(sessionNumber.toString) +: xs
      debug("SESS: sm")
      /*sender*/repSocket ! ZMQMessage(attachedMessage)
    }

    /**
     * メッセージを上層レイヤに送信する。
     *
     * 受信メッセージからセッション番号を取り出し、より上層のレイヤーに転送し、返答を待たせる。
     * 返答に自動的にセッション番号を復元してZeroMQソケットにメッセージを送信させる。
     */
    def forwardMessage(m: ZMQMessage, sender: ActorRef): Unit = {
      val (sessionNumber, unwrappedMessage) = detachSequenceNumber(m.frames)

      (listener ? unwrappedMessage).mapTo[Seq[ByteString]].onComplete {
        case Success(xs) =>
          debug("SESS: fm GOT RETRN")
          sendMessage(xs, sessionNumber, sender)
        case Failure(why) => warning(why.getMessage)
      }
    }

    /** `Seq[ByteString]`メッセージからセッション番号を取り出し、セッション番号とタプルにして返す。 */
    def detachSequenceNumber(xs: Seq[ByteString]): (Int, Seq[ByteString]) = {
      val sessionNumber = xs.head.decodeString("UTF-8").toInt
      (sessionNumber, xs.tail)
    }
  }

  class MessageListener extends Actor with ActorLogging {
    def receive = {
      case xs: Seq[ByteString] =>
        log.info("MLSTNR: " + xs.toString())
        val it = xs.map{bs => bs.mapI(_+1)}
        sender ! it
    }
  }

  override def main(args: Array[String]): Unit = {
    val system = ActorSystem("Exercise-ZMQ-Server")
    implicit def execContext: ExecutionContext = system.dispatcher
    val messageListener= system.actorOf(Props[MessageListener])
    val sessionLayer = system.actorOf(Props(classOf[SessionLayer], messageListener), name = "rep-session")

    println("test start on\ntcp://127.0.0.1:21231")
  }
}