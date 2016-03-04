package it.dtk.streaming

import java.util.concurrent.Executors

import akka.actor.Props
import it.dtk.es.ElasticQueryTerms
import it.dtk.model.{Feed, QueryTerm}
import it.dtk.streaming.receivers.ElasticQueryTermActor
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import it.dtk.dsl._
import org.joda.time.DateTime
import scala.concurrent.duration._

import scala.concurrent.ExecutionContext

/**
  * Created by fabiofumarola on 27/02/16.
  */
object ExtractQueryTerms extends StreamUtils {

  def main(args: Array[String]) {
    if (args.isEmpty) {
      println("mode: docker | local | prod")
      println(
        """
          |example
          | ./bin/spark-submit \
          |  --class it.dtk.streaming.ExtractQueryTerms \
          |  --master spark://spark-master-0 \
          |  --executor-memory 2G \
          |  --total-executor-cores 5 \
          |  /path/to/examples.jar  prod
        """.stripMargin)
      sys.exit(1)
    }

    val clusterName = "wheretolive"
    val queryTermIndexPath = "wtl/query_terms"
    val feedsIndexPath = "wtl/feeds"
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
        esIPs = "es-data-1,es-data-2,es-data-3"
        kafkaBrokers = "kafka-1:9092,kafka-2:9092,kafka-3:9092"
    }

    conf.set("es.nodes.wan.only", "true")
    conf.set("es.nodes", esIPs)

    val ssc = new StreamingContext(conf, Seconds(10))

    val nodes = esIPs.split(",").map(_ + ":9300").mkString(",")
    val queryTermStream = ssc.actorStream[QueryTerm](
      Props(new ElasticQueryTermActor(nodes, queryTermIndexPath, clusterName)), "QueryTermReceiver"
    )

    queryTermStream.count().foreachRDD { rdd =>
      println(s"Got ${rdd.collect()(0)} query terms from elasticsearch")
    }

    val toCheckQueryTerms = queryTermStream
      .filter(_.timestamp.
        getOrElse(DateTime.now().minusMinutes(10)).isBeforeNow)

    val articles = toCheckQueryTerms
      .flatMap(q => terms.generateUrls(q.terms, q.lang, "wheretolive.it"))
      .flatMap(u => terms.getResultsAsArticles(u))
      .map(a => gander.mainContent(a))

    articles.print(5)

    writeToKafka(articles, kafkaBrokers, "query_term_extractor", topic)

    saveQueryTerms(nodes, queryTermIndexPath, clusterName,
      toCheckQueryTerms.map(q => q.copy(timestamp = Option(DateTime.now()))))

    val feedsToAdd = articles
      .map(a => (a.publisher, html.host(a.uri)))
      .filter(_._2.nonEmpty)
      .map(p => (p._1, p._2.get))
      .filter(p => !p._2.contains("comment"))
      .transform(_.distinct())
      .flatMap { case (publisher, url) =>
        val urlFeeds = html.findRss(url)
        urlFeeds.
          filterNot(_ contains "comment").map(url =>
          Feed(url, publisher, List.empty, Some(DateTime.now().minusMinutes(10))))
      }

    saveFeedsToElastic(feedsIndexPath, feedsToAdd)

    ssc.start()
    ssc.awaitTermination()

  }

  def saveQueryTerms(hosts: String, indexPath: String, clusterName: String,
                     dStream: DStream[QueryTerm]): Unit = {
    dStream.foreachRDD { rdd =>
      rdd.foreachPartition { it =>
        import com.sksamuel.elastic4s.ElasticDsl._
        implicit val ex = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))
        val es = ElasticQueryTerms.connection(hosts, indexPath, clusterName)
        es.bulkCreateUpdate(it.toSeq).await(30 seconds)
      }
    }
  }
}
