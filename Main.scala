package momijikawa.exercisezmq

import scala.concurrent.ExecutionContext

object Main extends App {
  import akka.actor._
  import akka.zeromq._
  import akka.util.ByteString
  import concurrent.duration._
  import collection.immutable.Seq

  override def main(args: Array[String]): Unit = {
    val system = ActorSystem("Exercise-ZMQ-Server")
    implicit def execContext: ExecutionContext = system.dispatcher

    val schedule_initialDuration = 1 second
    val schedule_interval = 2 seconds
    val topic: String = "ping"
    val message = "PINGING MESSAGE"

    val pubSocket = ZeroMQExtension(system).newSocket(SocketType.Pub,
      Bind("tcp://127.0.0.1:21231"))

    println("test start on\ntcp://127.0.0.1:21231")

    system.scheduler.schedule(schedule_initialDuration, schedule_interval, pubSocket,
      ZMQMessage(
        Seq(
          ByteString(topic), // 第一フレーム目にtopicを指定する
          ByteString(message))))
  }
}
