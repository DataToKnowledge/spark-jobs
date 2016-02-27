package it.dtk.streaming

import akka.actor.Props
import it.dtk.model.{SchedulerData, Article, Feed}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.joda.time.DateTime
import org.json4s.NoTypeHints
import org.json4s.ext.JodaTimeSerializers
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._
import org.elasticsearch.spark._
import it.dtk.dsl._


import scala.collection.mutable
import scala.util.Try

/**
  * Created by fabiofumarola on 27/02/16.
  */
object ExtractFeeds {

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
          |  --class it.dtk.jobs.ExtractFeeds \
          |  --master spark://spark-master-0 \
          |  --executor-memory 2G \
          |  --total-executor-cores 5 \
          |  /path/to/examples.jar \
          |  false wtl/feeds es-data-1
        """.stripMargin)
      sys.exit(1)
    }

    val local = Try(args(0).toBoolean).getOrElse(true)
    val indexPath = Try(args(1)).getOrElse("wtl/feeds")
    val esNodes = Try(args(2)).getOrElse("192.168.99.100")
    val clusterName = "wtl"

    val kafkaServers = Try(args(3)).getOrElse("192.168.99.100:9092")
    val kafkaTopic = Try(args(4)).getOrElse("feed_items")
    val clientId = Try(args(5)).getOrElse(this.getClass.getName)

    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")

    if (local)
      conf.setMaster("local[4]")
    conf.set("es.nodes.wan.only", "true")
    conf.set("es.nodes", esNodes)

    val ssc = new StreamingContext(conf, Minutes(15))

    val feedsStream = ssc.actorStream[Feed](
      Props(new ElasticActorReceiver(esNodes, indexPath, clusterName)), "FeedsReceiver")

    val toCheckFeeds = feedsStream
      .filter(_.schedulerData.time.isBeforeNow)

    val feedArticles = extractFeedItems(toCheckFeeds)
    saveFeedsElastic(indexPath, feedArticles.map(_._1))

    val mainContents = feedArticles.flatMap(_._2)
      .map(a => gander.mainContent(a))



  }

  /**
    *
    * @param stream of feeds
    * @return a stream for (feed, list[articles]
    *         for each feed extract its articles
    */
  def extractFeedItems(stream: DStream[Feed]): DStream[(Feed, List[Article])] = {
    stream.map { f =>

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
