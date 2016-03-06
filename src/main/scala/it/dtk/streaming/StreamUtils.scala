package it.dtk.streaming

import it.dtk.kafka.{KafkaWriter, ProducerProperties}
import it.dtk.model.{Article, Feed, Tweet, _}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.elasticsearch.spark._
import org.json4s.NoTypeHints
import org.json4s.ext.JodaTimeSerializers
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._
import java.io.ByteArrayOutputStream
import com.gensler.scalavro.types.AvroType


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

  def writeToKafkaAvro(dStream: DStream[Article], brokers: String, clientId: String, topic: String): Unit = {
    val props = ProducerProperties(brokers, topic, clientId)

       dStream.foreachRDD { rdd =>
         rdd.foreachPartition { it =>
           val articleAvroType = AvroType[Article]

           val writer = KafkaWriter.getConnection(props)
           val buf = new ByteArrayOutputStream()

           it.foreach { a =>
             println(s"sending to kafka news with uri ${a.uri}")
             articleAvroType.io.write(a, buf)
             writer.send(a.uri.getBytes(), buf.toByteArray)
             buf.reset()
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

  def writeTweetsToKafkaAvro(dStream: DStream[Tweet], brokers: String, clientId: String, topic: String): Unit = {
    val props = ProducerProperties(brokers, topic, clientId)
       dStream.foreachRDD { rdd =>

         rdd.foreachPartition { it =>
           val tweetAvroType = AvroType[Tweet]

           val writer = KafkaWriter.getConnection(props)
           val buf = new ByteArrayOutputStream()

           it.foreach { t =>
             println(s"sending to kafka tweet with id ${t.id}")
             tweetAvroType.io.write(t, buf)
             writer.send(t.id.getBytes(), buf.toByteArray)
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
