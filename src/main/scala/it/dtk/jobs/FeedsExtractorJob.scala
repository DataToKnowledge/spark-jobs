package it.dtk.jobs

import java.util.Properties
import java.util.concurrent.{TimeUnit, Future}

import it.dtk.KafkaUtils
import it.dtk.model.{SchedulerData, Article, Feed}
import org.apache.spark.rdd.RDD
import org.apache.spark.{TaskContext, SparkConf, SparkContext}
import org.elasticsearch.spark._
import org.joda.time.DateTime
import org.json4s.NoTypeHints
import org.json4s.ext.JodaTimeSerializers
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import dsl._
import org.apache.kafka.clients.producer._

import scala.util.Try

/**
  * Created by fabiofumarola on 21/02/16.
  */
object FeedsExtractorJob {

  def main(args: Array[String]) {
    if (args.isEmpty) {
      println(
        """specify: local indexPath, esNodes
          |   local: true | false
          |   indexPath: wtl/feeds
          |   esNodes: 192.168.99.100
          |
          |example
          |./bin/spark-submit \
          |  --class it.dtk.sparkjobs.FeedsExtractorJob \
          |  --master spark://spark-master-0 \
          |  --executor-memory 2G \
          |  --total-executor-cores 5 \
          |  /path/to/examples.jar \
          |  false wtl/feeds es-data-1
        """.stripMargin)
      sys.exit(1)
    }

    val local = Try(args(0).toBoolean).getOrElse(true)
    val indexPath = Try(args(1)).getOrElse("test/feeds") //.getOrElse("wtl/feeds")
    val esNodes = Try(args(2)).getOrElse("192.168.99.100")
    val kafkaServers = Try(args(3)).getOrElse("192.168.99.100:9092")
    val kafkaTopic = Try(args(4)).getOrElse("articles")
    val clientId = Try(args(5)).getOrElse(this.getClass.getName)

    val conf = new SparkConf()
      .setAppName(this.getClass.getName)

    if (local)
      conf.setMaster("local[4]")

    conf.set("es.nodes.wan.only", "true")
    conf.set("es.nodes", esNodes)

    val sc = new SparkContext(conf)

    val datasource = loadFeedSources(sc, indexPath)

    //    val filteredSource = datasource
    //      .filter(_.schedulerData.time.isBeforeNow)
    //    println(filteredSource.count())

    val extracted = getFeedItems(datasource)

    //save updated feedSources
    val extrFeedSources = extracted.map(_._1)

    //    saveFeedSources(sc, indexPath, extrFeedSources)

    //extract main articles
    val extrArticles = extracted.flatMap(_._2)

    val array = extrArticles.collect()

    val mainArticles = getMainArticles(extrArticles)

    //save to kafka
    val offsets = writeToKafka(mainArticles, kafkaServers, clientId, kafkaTopic)

    println(s"extracted  feeds")

    sc.stop()
  }

  /**
    * parses the feed and extract the base article description from the feeds
    *
    * @param rdd
    * @return
    */
  def getFeedItems(rdd: RDD[Feed]): RDD[(Feed, List[Article])] = {
    rdd.map { f =>

      val filtArticles = try {
        val articles = feedExtr.parse(f.url, f.publisher)
        val setOfParsed = f.parsedUrls.toSet
        articles.filterNot(a => setOfParsed.contains(a.uri)).toList
      } catch {
        case e: Exception =>
          println(s"error in reading data from ${f.url} with error ${e.getMessage}")
          List.empty[Article]
      }

      val sizeFiltered = filtArticles.size

      val nextSched = SchedulerData.next(f.schedulerData, sizeFiltered)
      val parsedUrls = filtArticles.map(_.uri) ::: f.parsedUrls

      val newF = f.copy(
        lastTime = Option(DateTime.now),
        parsedUrls = parsedUrls.take(500),
        count = f.count + sizeFiltered,
        schedulerData = nextSched
      )

      (newF, filtArticles)
    }
  }

  def getMainArticles(rdd: RDD[Article]): RDD[Article] = {
    rdd.map(a => gander.extend(a))
  }

  def loadFeedSources(sc: SparkContext, indexPath: String): RDD[Feed] = {
    sc.esJsonRDD(indexPath)
      .map { id_json =>
        implicit val formats = Serialization.formats(NoTypeHints) ++ JodaTimeSerializers.all
        parse(id_json._2).extract[Feed]
      }
  }

  def saveFeedSources(sc: SparkContext, indexPath: String, rdd: RDD[Feed]): Unit = {
    rdd.map { feed =>
      implicit val formats = Serialization.formats(NoTypeHints) ++ JodaTimeSerializers.all
      write(feed)
    }.saveJsonToEs(indexPath, Map("es.mapping.id" -> "url"))
  }

  def saveArticlesToKafka(rdd: RDD[Article], kafkaServers: String, clientId: String, topic: String): RDD[Long] = {
    rdd.mapPartitionsWithIndex { (id, it) =>
      val props = new Properties()
      props.put("bootstrap.servers", kafkaServers)
      //    props.put("compression.type", "snappy")
      props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
      val producer = new KafkaProducer[Array[Byte], Array[Byte]](props)

      val result = it.map { a =>
        implicit val formats = Serialization.formats(NoTypeHints) ++ JodaTimeSerializers.all
        //        val msg = KafkaUtils.producerRecord(topic, None, a.uri, write(a))
        val msg = new ProducerRecord[Array[Byte], Array[Byte]](topic, a.uri.getBytes, write(a).getBytes)
        val r = producer.send(msg).get(10, TimeUnit.SECONDS)
        r.offset()
      }

      producer.close(10, TimeUnit.MILLISECONDS)
      result
    }
  }

  def writeToKafka(rdd: RDD[Article], kafkaServers: String, clientId: String, topic: String): Unit = {

    def save(context: TaskContext, iter: Iterator[Article]): Unit =  {
      val props = new Properties()
      props.put("bootstrap.servers", kafkaServers)
      //    props.put("compression.type", "snappy")
      props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
      val producer = new KafkaProducer[Array[Byte], Array[Byte]](props)
      try {
        iter.foreach { a =>
          if (context.isInterrupted) sys.error("interrupted")
          implicit val formats = Serialization.formats(NoTypeHints) ++ JodaTimeSerializers.all
          val msg = new ProducerRecord[Array[Byte], Array[Byte]](topic, a.uri.getBytes, write(a).getBytes)
          val r = producer.send(msg).get(10, TimeUnit.SECONDS)
        }
      } finally {
        producer.close()
      }
    }
    rdd.context.runJob[Article,Unit](rdd, save _)
  }

  def saveArticlesToKafkaLocal(rdd: RDD[Article], kafkaServers: String, clientId: String, topic: String): Array[Long] = {
    val producer = KafkaUtils.kafkaWriter(kafkaServers, "")
    val result = rdd.collect().map { a =>
      implicit val formats = Serialization.formats(NoTypeHints) ++ JodaTimeSerializers.all
      val msg = KafkaUtils.producerRecord(topic, None, a.uri, write(a))
      val r = producer.send(msg).get(10, TimeUnit.SECONDS)
      r.offset()
    }

    producer.close(10, TimeUnit.MILLISECONDS)
    result
  }
}
