package it.dtk.streaming

import java.io.File

import _root_.Tutorial._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.HashMap
import scala.io.Source

/**
  * Created by fabiana on 2/27/16.
  */
object TwitterStreaming {

  def main(args: Array[String]) : Unit = {
    configureTwitterCredentials()

    val sparkConf = new SparkConf().
      setAppName("Streaming").
      setMaster("local[*]")
    // Create a local StreamingContext with a batch interval of 1 second.
    // The master requires 2 cores to prevent from a starvation scenario.
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    val tweets = TwitterUtils.createStream(ssc, None)

    tweets.getReceiver().

    val dstream= tweets.map(status => status.getUser)
    dstream.print()
    ssc.start()             // Start the computation
    ssc.awaitTermination()
  }

  def configureTwitterCredentials() = {
    val twitterPath = getClass.getResource("/twitter.txt").getPath
    println(twitterPath)
    val file = new File(twitterPath)
    if (!file.exists) {
      throw new Exception("Could not find configuration file " + file)
    }
    val lines = Source.fromFile(file.toString).getLines.filter(_.trim.size > 0).toSeq
    val pairs = lines.map(line => {
      val splits = line.split("=")
      if (splits.size != 2) {
        throw new Exception("Error parsing configuration file - incorrectly formatted line [" + line + "]")
      }
      (splits(0).trim(), splits(1).trim())
    })
    val map = new HashMap[String, String] ++= pairs
    val configKeys = Seq("consumerKey", "consumerSecret", "accessToken", "accessTokenSecret")
    println("Configuring Twitter OAuth")
    configKeys.foreach(key => {
      if (!map.contains(key)) {
        throw new Exception("Error setting OAuth authenticaion - value for " + key + " not found")
      }
      val fullKey = "twitter4j.oauth." + key
      System.setProperty(fullKey, map(key))

      println("\tProperty " + fullKey + " set as " + map(key))
    })
    println()
  }
}
