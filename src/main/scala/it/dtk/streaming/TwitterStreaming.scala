package it.dtk.streaming

import java.io.File

import it.dtk.streaming.receivers.CustomTwitterInputDstream
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.io.Source

/**
  * Created by fabiana on 2/27/16.
  */
object TwitterStreaming extends StreamUtils {

  def main(args: Array[String]): Unit = {

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
    val indexPath = "wtl/query_terms"
    //val topic = "feed_items"

    var esIPs = "192.168.99.100"
    var kafkaBrokers = "192.168.99.100:9092"

    val conf: SparkConf = new SparkConf()
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



    Logger.getRootLogger.setLevel(Level.ERROR)

    val ssc = configureStreamingContext(conf)

    TwitterStreaming.loadTwitterKeys()

    val query = loadQueryTerms(ssc, indexPath)
    .map(q => q.terms.mkString(" "))

    //TODO get followers
    TwitterStreaming.startStream(ssc, query, Nil)
  }

  def configureStreamingContext(conf: SparkConf) = {
    // Create a local StreamingContext with a batch interval of 1 minute.
    // The master requires 2 cores to prevent from a starvation scenario.
    new StreamingContext(conf, Seconds(1*60))
  }

  def startStream(ssc:StreamingContext, query: Seq[String], followers: Seq[Long]) = {

    //val ids = Seq(419918470L, 2453246745L) //LanotteFabiana e VittorioZucconi
    val tweets = new CustomTwitterInputDstream(ssc, None,  query, followers).filter(_.getLang == "it")

    // Print tweets batch count
    tweets.foreachRDD(rdd => {
      println("\nNew tweets %s:".format(rdd.count()))
    })

    //get users and tweetText
    val users_text = tweets.map(status =>
      //(status.getUser.getScreenName, status.getText, status.getGeoLocation, status.getPlace, status.getSource, status.getWithheldInCountries)
        (status.getUser.getScreenName, status.getText)
    )
    users_text.print()

    ssc.start() // Start the computation
    ssc.awaitTermination()
  }

  def loadTwitterKeys() = {
    val twitterPath = getClass.getResource("/twitter.properties").getPath

    val file = new File(twitterPath)
    if (!file.exists) {
      throw new Exception("Could not find configuration file " + file)
    }
    val lines = Source.fromFile(file.toString).getLines.filter(_.trim.size > 0)
    val props = lines.map(line => line.split("=")).map {
      case (scala.Array(k, v)) => (k.trim, v.trim)
      case line => throw new Exception("Error parsing configuration file - incorrectly formatted line [" + line + "]")
    }
    props.foreach {
      case (k: String, v: String) =>
        val fullKey = "twitter4j.oauth." + k
        println("\tProperty " + k + " set as " + v)
        System.setProperty(fullKey, v)
    }
  }
}
