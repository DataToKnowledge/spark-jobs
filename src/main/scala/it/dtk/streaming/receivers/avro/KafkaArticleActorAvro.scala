package it.dtk.streaming.receivers.avro

import java.io.ByteArrayInputStream

import akka.actor.Actor
import com.gensler.scalavro.types.AvroType
import it.dtk.kafka.{ConsumerProperties, KafkaReader}
import it.dtk.model.Article
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.receiver.ActorHelper

import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import org.apache.kafka.common.serialization.ByteArrayDeserializer

/**
  * Created by fabiofumarola on 28/02/16.
  */
class KafkaArticleActorAvro(props: ConsumerProperties, beginning: Boolean = false) extends Actor with ActorHelper {

  import context.dispatcher

  val articleAvroType = AvroType[Article]

  val consumer = new KafkaReader(props)

  if (beginning) {
    consumer.poll()
    try {
      consumer.consumer.seekToBeginning(new TopicPartition(props.topics, 0))
      consumer.consumer.seekToBeginning(new TopicPartition(props.topics, 1))
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }
  context.system.scheduler.schedule(0 millis, 100 millis, self, "start")

  override def receive: Receive = {

    case "start" =>

      consumer.poll().foreach { rec =>
        val url = new String(rec.key())
        articleAvroType.io.read(new ByteArrayInputStream(rec.value)) match {
          case Success(article) =>
            log.info("got article from actor")
            store(url -> article)

          case Failure(ex) => ex.printStackTrace()
        }
      }
    //      self ! "start"
  }

  override def postStop(): Unit = {
    consumer.close()
  }
}
