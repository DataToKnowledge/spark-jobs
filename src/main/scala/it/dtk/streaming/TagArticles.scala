package it.dtk.streaming

import it.dtk.nlp.DBpedia
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import it.dtk.model._
import it.dtk.dsl._

/**
  * Created by fabiofumarola on 28/02/16.
  */
object TagArticles extends StreamUtils {

  def main(args: Array[String]) {
    if (args.isEmpty) {
      println("mode: docker | local | prod")
      println(
        """
          |example
          | ./bin/spark-submit \
          |  --class it.dtk.streaming.TagArticles \
          |  --master spark://spark-master-0 \
          |  --executor-memory 2G \
          |  --total-executor-cores 5 \
          |  /path/to/examples.jar  prod
        """.stripMargin)
      sys.exit(1)
    }

    val clusterName = "wheretolive"
    val readTopic = "feed_items"
    val writeTopic = "articles"

    var kafkaBrokers = "192.168.99.100:9092"
    var dbPediaBaseUrl = "http://192.168.99.100:2230"
    val lang = "it"


    val conf = new SparkConf()
      .setAppName(this.getClass.getName)

    args(0) match {
      case "local" =>
        kafkaBrokers = "localhost:9092"
        dbPediaBaseUrl = "http://localhost:2230"
        conf.setMaster("local[*]")

      case "docker" =>
        conf.setMaster("local[*]")

      case "prod" =>
        kafkaBrokers = "kafka-1:9092,kafka-2:9092,kafka-3:9092"
        dbPediaBaseUrl = "http://dbpedia_it:2230"
    }

    val ssc = new StreamingContext(conf, Seconds(30))

    val inTopicSet = readTopic.split(",").toSet
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> kafkaBrokers,
      " auto.offset.reset" -> "smallest"
    )

    val feedItems = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, inTopicSet
    )

    val distinctArticles = feedItems
      .transform(rdd => rdd.distinct())
      .map { case (url, strArticle) =>
        implicit val formats = Serialization.formats(NoTypeHints)
        parse(strArticle).extract[Article]
      }

    val taggedArticles = distinctArticles
      .mapPartitions { it =>
        val dbpedia = DBpedia.getConnection(dbPediaBaseUrl, lang)

        it.map { a =>
          val titleAn = dbpedia.annotateText(a.title)
          val descrAn = dbpedia.annotateText(a.description)
          val textAn = dbpedia.annotateText(a.cleanedText)
          val mergedCatKey = (a.categories ++ a.keywords).mkString(" ")
          val keywordAn = dbpedia.annotateText(mergedCatKey)
          val annotations = titleAn ++ descrAn ++ textAn ++ keywordAn
          a.copy(annotations = annotations.toList)
        }
      }

    val enrichArticles = taggedArticles.mapPartitions { it =>
      val dbpedia = DBpedia.getConnection(dbPediaBaseUrl, lang)
      it.map { a =>
        val enriched = a.annotations.map(ann => dbpedia.enrichAnnotation(ann))
        a.copy(annotations = enriched)
      }
    }

    writeToKafka(enrichArticles, kafkaBrokers, "tag_articles", writeTopic)

    ssc.start()
    ssc.awaitTermination()
  }

}
