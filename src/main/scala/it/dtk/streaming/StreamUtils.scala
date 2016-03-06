package it.dtk.streaming

import java.io.ByteArrayOutputStream

import com.gensler.scalavro.types.AvroType
import it.dtk.model.{Article, Feed, Tweet, _}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.elasticsearch.spark._
import org.json4s.NoTypeHints
import org.json4s.ext.JodaTimeSerializers
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._

import scala.collection.JavaConversions._

/**
  * Created by fabiofumarola on 27/02/16.
  */
trait StreamUtils {

  def kafkaProducerProps(brokers: String, clientId: String) =
    Map[String, Object](
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArraySerializer",
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArraySerializer",
      ProducerConfig.ACKS_CONFIG -> "1",
      ProducerConfig.CLIENT_ID_CONFIG -> clientId
    )


  def writeToKafka(dStream: DStream[Article], brokers: String, clientId: String, topic: String): Unit = {
    val props = kafkaProducerProps(brokers, clientId)

    dStream.foreachRDD { rdd =>
      rdd.foreachPartition { it =>
        implicit val formats = Serialization.formats(NoTypeHints) ++ JodaTimeSerializers.all
        val producer = new KafkaProducer[Array[Byte], Array[Byte]](props)

        it.foreach { a =>
          println(s"sending to kafka news with uri ${a.uri}")
          val message = new ProducerRecord[Array[Byte], Array[Byte]](topic, a.uri.getBytes(), write(a).getBytes())
          producer.send(message)
        }
      }
    }
  }

  def writeToKafkaAvro(dStream: DStream[Article], brokers: String, clientId: String, topic: String): Unit = {
    val props = kafkaProducerProps(brokers, clientId)

    dStream.foreachRDD { rdd =>
      rdd.foreachPartition { it =>
        val articleAvroType = AvroType[Article]

        val producer = new KafkaProducer[Array[Byte], Array[Byte]](props)
        val buf = new ByteArrayOutputStream()

        it.foreach { a =>
          println(s"sending to kafka news with uri ${a.uri}")
          articleAvroType.io.write(a, buf)
          val message = new ProducerRecord[Array[Byte], Array[Byte]](topic, a.uri.getBytes(), buf.toByteArray)
          producer.send(message)
          buf.reset()
        }
      }
    }
  }

  def writeTweetsToKafka(dStream: DStream[Tweet], brokers: String, clientId: String, topic: String): Unit = {
    val props = kafkaProducerProps(brokers, clientId)

    dStream.foreachRDD { rdd =>

      rdd.foreachPartition { it =>
        implicit val formats = Serialization.formats(NoTypeHints) ++ JodaTimeSerializers.all
        val producer = new KafkaProducer[Array[Byte], Array[Byte]](props)

        it.foreach { t =>
          println(s"sending to kafka tweet with id ${t.id}")
          val message = new ProducerRecord[Array[Byte], Array[Byte]](topic, t.id.getBytes(), write(t).getBytes())
          producer.send(message)
        }
      }
    }
  }

  def writeTweetsToKafkaAvro(dStream: DStream[Tweet], brokers: String, clientId: String, topic: String): Unit = {
    val props = kafkaProducerProps(brokers, clientId)

    dStream.foreachRDD { rdd =>

      rdd.foreachPartition { it =>
        val tweetAvroType = AvroType[Tweet]

        val producer = new KafkaProducer[Array[Byte], Array[Byte]](props)
        val buf = new ByteArrayOutputStream()

        it.foreach { t =>
          println(s"sending to kafka tweet with id ${t.id}")
          tweetAvroType.io.write(t, buf)
          val message = new ProducerRecord[Array[Byte], Array[Byte]](topic, t.id.getBytes(), write(t).getBytes())
          producer.send(message)
          buf.reset()
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
  def saveFeedsToElastic(indexPath: String, dStream: DStream[Feed]): Unit = {
    dStream.foreachRDD { rdd =>
      rdd.map { feed =>
        implicit val formats = Serialization.formats(NoTypeHints) ++ JodaTimeSerializers.all
        write(feed)
      }.saveJsonToEs(indexPath, Map("es.mapping.id" -> "url"))
    }
  }


  def saveArticleToElastic(indexPath: String, dStream: DStream[Article]): Unit = {
    dStream.foreachRDD { rdd =>
      rdd.map { a =>
        implicit val formats = Serialization.formats(NoTypeHints) ++ JodaTimeSerializers.all
        println("save article to es")
        write(a)
      }.saveJsonToEs(indexPath, Map("es.mapping.id" -> "uri"))
    }
  }

  def loadQueryTerms(ssc: StreamingContext, indexPath: String): Array[QueryTerm] = {
    ssc.sparkContext.esJsonRDD(indexPath)
      .map { id_json =>
        implicit val formats = Serialization.formats(NoTypeHints) ++ JodaTimeSerializers.all
        parse(id_json._2).extract[QueryTerm]
      }.collect()
  }
}
