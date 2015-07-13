package momijikawa.exercisezmq

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

  /** 受信したメッセージをラップし全て`upperLayer`に転送するアクター。 */
  class RepActor(upperLayer: ActorRef) extends Actor with ActorLogging {
    import log.{ debug, warning }

    def receive = {
      case m: ZMQMessage =>
        debug(s"message inbound: ${m.frames}")
        upperLayer ! RxRaw(m)

      case unknown => warning("Unknown: $unknown")
    }
  }

  /**
   * メッセージのセッション番号を自動的に取り外し・付与し、受信したメッセージと返答を返すアクターとの仲介を行う。
   *
   * @param listener 受信したメッセージの転送先アクター。
   *
   * ZeroMQのRepソケットを作成し、受信するメッセージを自分に転送させる。メッセージにはセッション番号が含まれているため、
   * これを取り外し、`listener`に転送する。`listener`がメッセージを返信したら、そのメッセージにセッション番号を付与して
   * ZeroMQのRepソケットに送信させる。
   */
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

    /** 返信メッセージにセッション番号を付与し送信させる。 */
    def sendMessage(xs: Seq[ByteString], sessionNumber: Int): Unit = {
      val attachedMessage: Seq[ByteString] = ByteString(sessionNumber.toString) +: xs
      repSocket ! ZMQMessage(attachedMessage)
    }

    /**
     * メッセージを上層レイヤに送信し、メッセージが返って来たらReqへの返信を自動的に行う。
     *
     * 受信メッセージからセッション番号を取り出し、より上層のレイヤーに転送し、返答を待たせる。
     * 返答に自動的にセッション番号を復元してZeroMQソケットにメッセージを送信させる。
     */
    def forwardMessage(m: ZMQMessage, sender: ActorRef): Unit = {
      val (sessionNumber, unwrappedMessage) = detachSequenceNumber(m.frames)

      (listener ? unwrappedMessage).mapTo[Seq[ByteString]].onComplete {
        case Success(xs) => sendMessage(xs, sessionNumber)
        case Failure(why) => warning(why.getMessage)
      }
    }

    /** `Seq[ByteString]`メッセージからセッション番号を取り出し、セッション番号とタプルにして返す。 */
    def detachSequenceNumber(xs: Seq[ByteString]): (Int, Seq[ByteString]) = {
      val sessionNumber = xs.head.decodeString("UTF-8").toInt
      (sessionNumber, xs.tail)
    }
  }

  /** 実際のアプリケーション役となるアクター。メッセージのバイト列に1を足して返信する。 */
  class MessageListener extends Actor with ActorLogging {
    def receive = {
      case xs: Seq[ByteString] =>
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