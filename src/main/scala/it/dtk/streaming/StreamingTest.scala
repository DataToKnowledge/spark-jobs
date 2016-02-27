package it.dtk.streaming

import akka.actor.Props
import it.dtk.KafkaActorReceiver
import it.dtk.KafkaActorReceiver.ConsumerRecord
import it.dtk.kafka.ConsumerProperties
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by fabiofumarola on 25/02/16.
  */
object StreamingTest {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("Test").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    val consProps = ConsumerProperties(
      brokers = "192.168.99.100:9092",
      topics = "feed_items",
      groupName = "feed_items"
    )

    val feeds = ssc.actorStream[ConsumerRecord](Props(new KafkaActorReceiver(consProps)), "KafkaReceiver")

    feeds
      .map(r => (new String(r.key), new String(r.value)))
      .print()

    ssc.start()
    ssc.awaitTermination()
  }

}