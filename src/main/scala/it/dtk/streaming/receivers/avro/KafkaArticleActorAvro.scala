package it.dtk.streaming.receivers.avro

import java.io.ByteArrayInputStream

import akka.actor.{PoisonPill, Actor}
import com.gensler.scalavro.types.AvroType
import it.dtk.model.Article
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.spark.streaming.receiver.ActorHelper

import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
  * Created by fabiofumarola on 28/02/16.
  */
class KafkaArticleActorAvro(props: Map[String, String], topic: String, beginning: Boolean = false) extends Actor with ActorHelper {

  import context.dispatcher
  import org.apache.kafka.common.serialization.ByteArrayDeserializer

  val deserializer = new ByteArrayDeserializer

  val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](props)
  //  consumer.subscribe(topic.split(",").toList)

  if (beginning) {
    consumer.poll(100)
    try {
      //      consumer.seekToBeginning(new TopicPartition(topic, 0))
      //      consumer.seekToBeginning(new TopicPartition(topic, 1))
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }
  context.system.scheduler.schedule(0 millis, 100 millis, self, "start")

  override def receive: Receive = {

    case "start" =>
      consumer.poll(100).foreach { rec =>
        //        store((rec.key(), rec.value()))
      }
  }

  override def postStop(): Unit = {
    consumer.close()
  }
}
