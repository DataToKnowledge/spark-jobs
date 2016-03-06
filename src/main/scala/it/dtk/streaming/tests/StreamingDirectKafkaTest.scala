package it.dtk.jobs.tests

import org.apache.commons.codec.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by fabiofumarola on 24/02/16.
  */
object StreamingDirectKafkaTest {

  def main(args: Array[String]) {

    val brokers = "192.168.99.100:9092"
    val topics = "feed_items"

    val sparkConf = new SparkConf().
      setAppName("Streaming").
      setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.checkpoint("checkpoint")

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, " auto.offset.reset" -> "smallest")
//    val articles = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
//      ssc, kafkaParams, topicsSet)
//
//    articles.foreachRDD { rdd =>
//      val array = rdd.collect()
//      println(array.length)
//    }

    ssc.start()
    ssc.awaitTermination()
  }
}

