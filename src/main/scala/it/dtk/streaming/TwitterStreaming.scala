package it.dtk.streaming

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.io.Source

/**
  * Created by fabiana on 2/27/16.
  */
object TwitterStreaming {

  def main(args: Array[String]): Unit = {
    loadTwitterKeys()

    val sparkConf = new SparkConf().
      setAppName("StreamingTwitter").
      setMaster("local[*]")
    // Create a local StreamingContext with a batch interval of 1 minute.
    // The master requires 2 cores to prevent from a starvation scenario.
    val ssc = new StreamingContext(sparkConf, Seconds(1 * 60))

    //    val filtre = new FilterQuery();
    //    filtre.follow(usuarios)
    val query = Seq("bari", "roma", "milano")

    val tweets = TwitterUtils.createStream(ssc, None, query).filter(_.getLang == "it")

    val dstream = tweets.map(status => (status.getUser.getId, status.getText))
    dstream.print()
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
