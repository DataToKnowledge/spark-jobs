package it.dtk.streaming.receivers

import akka.actor.Actor
import akka.actor.Actor.Receive
import it.dtk.kafka.{KafkaReader, ConsumerProperties}
import it.dtk.model.Article
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.receiver.ActorHelper
import org.json4s.NoTypeHints
import org.json4s.ext.JodaTimeSerializers
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import scala.concurrent.duration._
import collection.JavaConversions._

/**
  * Created by fabiofumarola on 28/02/16.
  */
class KafkaFeedItemsActor(props: ConsumerProperties, beginning: Boolean = false) extends Actor with ActorHelper {
  implicit val formats = Serialization.formats(NoTypeHints) ++ JodaTimeSerializers.all

  import context.dispatcher

  val consumer = new KafkaReader[Array[Byte], Array[Byte]](props)
  consumer.poll()
  consumer.consumer.seekToBeginning(new TopicPartition(props.topics,0))

  context.system.scheduler.scheduleOnce(10 millis, self, "start")

  override def receive: Receive = {

    case "start" =>
      consumer.poll().foreach { rec =>
        val url = new String(rec.key())
        val article = parse(new String(rec.value())).extract[Article]
        store(url -> article)
      }
      self ! "start"
  }

  override def postStop(): Unit = {
    consumer.close()
  }
}
