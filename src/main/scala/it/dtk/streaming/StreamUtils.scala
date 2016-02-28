package it.dtk.streaming

import it.dtk.kafka.{KafkaWriter, ProducerProperties}
import it.dtk.model.{Tweet, Article, Feed}
import org.apache.spark.streaming.dstream.DStream
import org.elasticsearch.spark._
import org.json4s.NoTypeHints
import org.json4s.ext.JodaTimeSerializers
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._

/**
  * Created by fabiofumarola on 27/02/16.
  */
trait StreamUtils {

  def writeToKafka(dStream: DStream[Article], brokers: String, clientId: String, topic: String): Unit = {
    val props = ProducerProperties(brokers, topic, clientId)
    dStream.foreachRDD { rdd =>

      rdd.foreachPartition { it =>
        implicit val formats = Serialization.formats(NoTypeHints) ++ JodaTimeSerializers.all

        val writer = KafkaWriter.getConnection(props)

        it.foreach { a =>
          println(s"sending to kafka news with uri ${a.uri}")
          writer.send(a.uri.getBytes(), write(a).getBytes())
        }
      }
    }
  }

  def writeTweetsToKafka(dStream: DStream[Tweet], brokers: String, clientId: String, topic: String): Unit = {
    val props = ProducerProperties(brokers, topic, clientId)
    dStream.foreachRDD { rdd =>

      rdd.foreachPartition { it =>
        implicit val formats = Serialization.formats(NoTypeHints) ++ JodaTimeSerializers.all

        val writer = KafkaWriter.getConnection(props)

        it.foreach { t =>
          println(s"sending to kafka tweet with id ${t.id}")
          writer.send(t.id.getBytes(), write(t).getBytes())
        }
      }
    }
  }

  /**
    * upsert the given feeds into elasticsearch
    *
    * @param indexPath
    * @param dStream
    *
    */
  def saveFeedsElastic(indexPath: String, dStream: DStream[Feed]): Unit = {
    dStream.foreachRDD { rdd =>
      rdd.map { feed =>
        implicit val formats = Serialization.formats(NoTypeHints) ++ JodaTimeSerializers.all
        write(feed)
      }.saveJsonToEs(indexPath, Map("es.mapping.id" -> "url"))
    }
  }
}
