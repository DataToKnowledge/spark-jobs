package it.dtk.sparkjobs

import it.dtk.sparkler.FakeDB
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
object SaveFeedsJob {
  def main(args: Array[String]): Unit = {


    val local = true
    val indexPath = "wheretolive/feeds"
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
    val rdd = sc.esRDD(indexPath)
    rdd.foreach(println)

    sc.stop()
  }
}
