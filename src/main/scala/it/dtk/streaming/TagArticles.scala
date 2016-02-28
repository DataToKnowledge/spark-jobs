package it.dtk.streaming

import akka.actor.Props
import it.dtk.kafka.ConsumerProperties
import it.dtk.model._
import it.dtk.nlp.{DBpediaSpotLight, DBpedia}
import it.dtk.streaming.receivers.KafkaFeedItemsActor
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

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

    val ssc = new StreamingContext(conf, Seconds(10))

    val consProps = ConsumerProperties(
      brokers = kafkaBrokers,
      topics = readTopic,
      groupName = "tag_articles"
    )

    val feedItemStream = ssc.actorStream[(String, Article)](
      Props(new KafkaFeedItemsActor(consProps)), "read_articles"
    )

    val distinctArticles = feedItemStream
      .transform(rdd => rdd.distinct())
      .map(_._2)


    val taggedArticles = distinctArticles
      .mapPartitions { it =>
        val dbpedia = DBpedia.getConnection(dbPediaBaseUrl, lang)
        it.map(a => annotateArticle(dbpedia, a))
      }

    val enrichedArticles = taggedArticles.mapPartitions { it =>
      val dbpedia = DBpedia.getConnection(dbPediaBaseUrl, lang)
      it.map { a =>
        val enriched = a.annotations.map(ann => dbpedia.enrichAnnotation(ann))
        a.copy(annotations = enriched)
      }
    }

    val locationArticles = enrichedArticles.mapPartitions{ it =>

    }

    writeToKafka(enrichedArticles, kafkaBrokers, "tag_articles", writeTopic)

    ssc.start()
    ssc.awaitTermination()
  }

  def annotateArticle(dbpedia: DBpediaSpotLight, a: Article): Article = {

    val titleAn = if (a.title.nonEmpty)
      dbpedia.annotateText(a.title)
    else Seq.empty[Annotation]

    val descrAn = if (a.description.nonEmpty)
      dbpedia.annotateText(a.description)
    else Seq.empty[Annotation]

    val textAn = if (a.cleanedText.nonEmpty)
      dbpedia.annotateText(a.cleanedText)
    else Seq.empty[Annotation]

    val mergedCatKey = (a.categories ++ a.keywords).mkString(" ")

    val keywordAn = if (mergedCatKey.nonEmpty)
      dbpedia.annotateText(mergedCatKey)
    else Seq.empty[Annotation]

    val annotations = titleAn ++ descrAn ++ textAn ++ keywordAn
    a.copy(annotations = annotations.toList)
  }

}


//    val inTopicSet = readTopic.split(",").toSet
//    val kafkaParams = Map[String, String](
//      "bootstrap.servers" -> kafkaBrokers,
//      "group.id" -> "feed_items",
//      "enable.auto.commit" -> "true",
//      "key.deserializer" -> "org.apache.kafka.common.serialization.ByteArrayDeserializer",
//      "value.deserializer" -> "org.apache.kafka.common.serialization.ByteArrayDeserializer"
//    )
//
//    val feedItems = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
//      ssc, kafkaParams, inTopicSet
//    )