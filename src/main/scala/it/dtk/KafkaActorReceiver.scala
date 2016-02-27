package it.dtk

import akka.actor.Actor
import it.dtk.kafka.{ConsumerProperties, KafkaReader}
import org.apache.spark.streaming.receiver.ActorHelper
import scala.concurrent.duration._
import scala.collection.JavaConversions._

object KafkaActorReceiver {

  case class ConsumerRecord(key: Array[Byte], value: Array[Byte])

}

/**
  * Created by fabiofumarola on 25/02/16.
  */
class KafkaActorReceiver(consProps: ConsumerProperties, timeout: Long = 100) extends Actor with ActorHelper {
  import context.dispatcher
  import KafkaActorReceiver._

  val reader = new KafkaReader[Array[Byte], Array[Byte]](consProps)

  context.system.scheduler.scheduleOnce(100.millisecond, self, "poll")

  self ! "poll"

  override def receive: Receive = {

    case "poll" =>
      val records = reader.consume(timeout)
      records.foreach(rec => self ! ConsumerRecord(rec.key(), rec.value()))

    case c: ConsumerRecord =>
      store(c)
      self ! "poll"
  }
}
