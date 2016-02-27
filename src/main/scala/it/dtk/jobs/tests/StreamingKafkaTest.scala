package it.dtk.jobs.tests

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by fabiofumarola on 24/02/16.
  */
object StreamingKafkaTest {

  def main(args: Array[String]) {

    val zk = "192.168.99.100:2181"
    val brokers = "192.168.99.100:9092"
    val topics = "feed_items"

    val sparkConf = new SparkConf().
      setAppName("Streaming").
      setMaster("local[4]")

    val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.checkpoint("checkpoint")

    val topicMap = topics.split(",").map((_, 1)).toMap
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, " auto.offset.reset" -> "smallest")
    val articles = KafkaUtils.createStream(
      ssc, zk, "test",topicMap, StorageLevel.MEMORY_ONLY)

    articles.foreachRDD { rdd =>
      val array = rdd.collect()
      println(array.length)
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
