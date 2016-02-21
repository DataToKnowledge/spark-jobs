package it.dtk.sparkler.jobs

import it.dtk.sparkler.FakeDB
import it.dtk.sparkler.dsl._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by fabiofumarola on 30/01/16.
  */
object FeedsJob {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Feeds Job")
      .setMaster("local[4]")
    val sc = new SparkContext(conf)

    val sourceList = FakeDB.listFeeds()

    val romeArts = sc.parallelize(sourceList)
      .flatMap(src => rome.parse(src.url, src.publisher)
        .filter(art => !src.parsedUrls.contains(art.uri)))

    val articles = romeArts.map(gander.extend)

    articles.foreach { a =>
      println(s"${a.uri} ${a.keywords}")
    }


    val sources = articles
      .map(_.uri)
      .map(u => html.host(u))
      .filter(_.nonEmpty)
      .map(_.get)
      .filter(f => !f.contains("comments"))
      .distinct()
      .map(url => (url, html.findRss(url)))


    sources.collect().foreach {
      case (url, rss) =>
        println(s" for url $url feeds $rss ")
    }

    sc.stop()
  }

}
