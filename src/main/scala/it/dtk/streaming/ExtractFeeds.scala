package it.dtk.streaming

import akka.actor.Props
import it.dtk.dsl._
import it.dtk.model.{Article, Feed, SchedulerData}
import it.dtk.streaming.receivers.ElasticActorReceiver
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, Minutes, StreamingContext}
import org.joda.time.DateTime


/**
  * Created by fabiofumarola on 27/02/16.
  */
object ExtractFeeds extends StreamUtils {

  def main(args: Array[String]) {

    if (args.isEmpty) {
      println("mode: docker | local | prod")
      println(
        """
          |example
          | ./bin/spark-submit \
          |  --class it.dtk.jobs.ExtractFeeds \
          |  --master spark://spark-master-0 \
          |  --executor-memory 2G \
          |  --total-executor-cores 5 \
          |  /path/to/examples.jar  prod
        """.stripMargin)
      sys.exit(1)
    }

    val clusterName = "wheretolive"
    val indexPath = "wtl/feeds"
    val topic = "feed_items"

    var esIPs = "192.168.99.100"
    var kafkaBrokers = "192.168.99.100:9092"

    val conf = new SparkConf()
      .setAppName(this.getClass.getName)

    args(0) match {
      case "local" =>
        esIPs = "localhost"
        kafkaBrokers = "localhost:9092"
        conf.setMaster("local[*]")

      case "docker" =>
        conf.setMaster("local[*]")

      case "prod" =>
        esIPs = "es-data-0,es-data-1,es-data-2"
        kafkaBrokers = "kafka-1:9092,kafka-2:9092,kafka-3:9092"
    }

    conf.set("es.nodes.wan.only", "true")
    conf.set("es.nodes", esIPs)

    val ssc = new StreamingContext(conf, Seconds(30))

    val nodes = esIPs.split(",").map(_ + ":9300").mkString(",")
    val feedsStream = ssc.actorStream[Feed](
      Props(new ElasticActorReceiver(nodes, indexPath, clusterName)), "FeedsReceiver")

    feedsStream.count().foreachRDD { rdd =>
      println(s"Got ${rdd.collect()(0)} feeds from elasticsearch")
    }

    val toCheckFeeds = feedsStream
      .filter(_.schedulerData.time.isBeforeNow)

    val feedArticles = extractFeedItems(toCheckFeeds)
    saveFeedsElastic(indexPath, feedArticles.map(_._1))

    val mainContents = feedArticles.flatMap(_._2)
      .map(article => gander.mainContent(article))

    writeToKafka(mainContents, kafkaBrokers, "feed_extractor", topic)

    ssc.start()
    ssc.awaitTermination()
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
}
