package it.dtk.streaming.receivers

import java.io.ByteArrayInputStream

import akka.actor.Props
import com.gensler.scalavro.types.AvroType
import consumer.kafka.ReceiverLauncher
import it.dtk.model._
import it.dtk.nlp.{DBpedia, DBpediaSpotLight, FocusLocation}
import it.dtk.streaming.StreamUtils
import it.dtk.streaming.receivers.avro.KafkaArticleActorAvro
import org.apache.hadoop.hdfs.server.common.Storage
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
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
    val zkhosts = "zoo-1,zoo-2,zoo-3"
    val zkports = "2181"
    val brokerPath = "/brokers"

    var kafkaBrokers = "192.168.99.100:9092"
    var dbPediaBaseUrl = "http://192.168.99.100:2230"
    val lang = "it"

    var esIPs = "192.168.99.100"
    val locDocPath = "wtl/locations"


    val conf = new SparkConf()
      .setAppName(this.getClass.getName)

    args(0) match {
      case "local" =>
        kafkaBrokers = "localhost:9092"
        dbPediaBaseUrl = "http://localhost:2230"
        esIPs = "locahost"
        conf.setMaster("local[*]")

      case "docker" =>
        conf.setMaster("local[*]")

      case "prod" =>
        esIPs = "es-data-1,es-data-2,es-data-3"
        kafkaBrokers = "kafka-1:9092,kafka-2:9092,kafka-3:9092"
        dbPediaBaseUrl = "http://dbpedia_it"

    }

    val ssc = new StreamingContext(conf, Seconds(10))
    //    ssc.checkpoint("/tmp")
    val consProps = Map(
      "bootstrap.servers" -> kafkaBrokers,
      "group.id" -> "feed_reader",
      "value.deserializer" -> "org.apache.kafka.common.serialization.ByteArrayDeserializer",
      "key.deserializer" -> "org.apache.kafka.common.serialization.ByteArrayDeserializer",
      "partition.assignment.strategy" -> ""
    )

    val kafkaProperties: Map[String, String] = Map("zookeeper.hosts" -> zkhosts,
      "zookeeper.port" -> zkports,
      "zookeeper.broker.path" -> brokerPath,
      "kafka.topic" -> readTopic,
      "zookeeper.consumer.connection" -> "zoo-2:2181",
      "zookeeper.consumer.path" -> "/spark-kafka",
      "kafka.consumer.id" -> "feed_reader",
      //optional properties
      "consumer.forcefromstart" -> "true",
      "consumer.backpressure.enabled" -> "true",
      "consumer.fetchsizebytes" -> "1048576",
      "consumer.fillfreqms" -> "250")

    val props = new java.util.Properties()
    kafkaProperties foreach { case (key, value) => props.put(key, value) }

    val inputStream = ReceiverLauncher.launch(ssc, props, 1, StorageLevel.MEMORY_ONLY).
      map(m => m.getKey -> m.getPayload)

    //    val inputStream = ssc.actorStream[(Array[Byte], Array[Byte])](
    //      Props(new KafkaArticleActorAvro(consProps, readTopic, true)), "read_articles"
    //    )

    inputStream.count().foreachRDD { rdd =>
      println(s"Got ${rdd.collect()(0)} articles to tag from kafka")
    }

    val feedItemStream = inputStream.mapPartitions { it =>
      val articleAvroType = AvroType[Article]
      it.map { kv =>
        val url = new String(kv._1)
        val article = articleAvroType.io.read(new ByteArrayInputStream(kv._2)).toOption

        url -> article
      }
    }.filter(_._2.isDefined)
      .map(kv => kv._1 -> kv._2.get)

    feedItemStream.print(1)

    val distinctArticles = feedItemStream
      .transform(rdd => rdd.distinct())
      .map(_._2)


    val taggedArticles = distinctArticles
      .mapPartitions { it =>
        val dbpedia = DBpedia.getConnection(dbPediaBaseUrl, lang)
        it.map(a => annotateArticle(dbpedia, a))
      }

    taggedArticles.print(1)

    val enrichedArticles = taggedArticles.mapPartitions { it =>
      val dbpedia = DBpedia.getConnection(dbPediaBaseUrl, lang)
      val result = it.map { a =>
        val enriched = a.annotations.map(ann => dbpedia.enrichAnnotation(ann))
        a.copy(annotations = enriched)
      }
      //      DBpedia.closePool()
      result
    }

    enrichedArticles.print(1)

    val nodes = esIPs.split(",").map(_ + ":9300").mkString(",")


    val locationArticles = enrichedArticles.mapPartitions { it =>
      val focusLoc = FocusLocation.getConnection(nodes, locDocPath, clusterName)

      val result = it.map { a =>
        val location = focusLoc.extract(a)
        a.copy(focusLocation = location)
      }
      result
    }

    writeToKafkaAvro(locationArticles, kafkaBrokers, "tag_articles", writeTopic)

    ssc.start()
    ssc.awaitTermination()
  }

  def annotateArticle(dbpedia: DBpediaSpotLight, a: Article): Article = {

    val titleAn = if (a.title.nonEmpty)
      dbpedia.annotateText(a.title, DocumentSection.Title)
    else Seq.empty[Annotation]

    val descrAn = if (a.description.nonEmpty)
      dbpedia.annotateText(a.description, DocumentSection.Summary)
    else Seq.empty[Annotation]

    val textAn = if (a.cleanedText.nonEmpty)
      dbpedia.annotateText(a.cleanedText, DocumentSection.Corpus)
    else Seq.empty[Annotation]

    val mergedCatKey = (a.categories ++ a.keywords).mkString(" ")

    val keywordAn = if (mergedCatKey.nonEmpty)
      dbpedia.annotateText(mergedCatKey, DocumentSection.KeyWords)
    else Seq.empty[Annotation]

    val annotations = titleAn ++ descrAn ++ textAn ++ keywordAn
    a.copy(annotations = annotations.toList)
  }
}