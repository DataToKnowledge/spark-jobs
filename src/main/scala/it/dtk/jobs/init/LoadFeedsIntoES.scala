package it.dtk.jobs.init

import it.dtk.model.Feed
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._
import org.json4s._
import org.json4s.ext.JodaTimeSerializers
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write

/**
  * Created by fabiofumarola on 15/02/16.
  */
object LoadFeedsIntoES {
  def main(args: Array[String]): Unit = {


    val local = true
    val indexPath = "wtl/feeds"
    val esNodes = "192.168.99.100"

    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .set("es.nodes.wan.only", "true")
      .set("es.nodes", esNodes)

    if (local)
      conf.setMaster("local[4]")

    val sc = new SparkContext(conf)

    val feeds = sc.parallelize(FakeDB.listFeeds())
    feeds
      .map { feed =>
        implicit val formats = Serialization.formats(NoTypeHints) ++ JodaTimeSerializers.all
        write(feed)
      }
      .saveJsonToEs(indexPath, Map("es.mapping.id" -> "url"))
    val rdd = sc.esJsonRDD(indexPath)

    val parsedFeed = rdd.map{
      case (id, json) =>
        implicit val formats = Serialization.formats(NoTypeHints) ++ JodaTimeSerializers.all
        parse(json).extract[Feed]
    }

    parsedFeed.foreach(println)

    sc.stop()
  }
}
