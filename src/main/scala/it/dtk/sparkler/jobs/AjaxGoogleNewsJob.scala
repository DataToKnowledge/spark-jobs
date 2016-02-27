package it.dtk.sparkler.jobs

import it.dtk.sparkler.dsl._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by fabiofumarola on 31/01/16.
  */
object AjaxGoogleNewsJob {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Ajax Google News Job")
      .setMaster("local[4]")

    val sc = new SparkContext(conf)

    val source = List(
      List("furto"),
      List("rapina"),
      List("omicidio")
    )

    val lang = "it"
    val ip = "151.48.44.113"

    val news = sc.parallelize(source)
      .flatMap(terms => googleNews.generateUrls(terms, lang, ip))
      .flatMap(u => googleNews.getResultsAsArticles(u))
      .map(a =>gander.mainContent(a))

    val collected = news.collect()

    collected.foreach { c =>
      println(c)
    }

    sc.stop()
  }

}
