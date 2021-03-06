package it.dtk.streaming

import java.io.ByteArrayInputStream

import com.gensler.scalavro.types.AvroType
import it.dtk.model._
import it.dtk.nlp.{DBpedia, DBpediaSpotLight, FocusLocation}
import kafka.serializer.DefaultDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Try

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
        esIPs = "es-data-2,es-data-3,es-data-4"
        kafkaBrokers = "kafka-1:9092,kafka-2:9092,kafka-3:9092"
        dbPediaBaseUrl = "http://dbpedia_it"
    }

    val offset = Try(args(1)).getOrElse("largest")

    val ssc = new StreamingContext(conf, Seconds(10))

    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> kafkaBrokers,
      "auto.offset.reset" -> offset)

    val inputStream = KafkaUtils.createDirectStream[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder](
      ssc, kafkaParams, readTopic.split(",").toSet
    )

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


    val distinctArticles = feedItemStream
      .transform(rdd => rdd.distinct())
      .map(_._2)


    val taggedArticles = distinctArticles
      .mapPartitions { it =>
        val dbpedia = DBpedia.getConnection(dbPediaBaseUrl, lang)
        it.map(a => annotateArticle(dbpedia, a))
      }

    taggedArticles.count().foreachRDD { rdd =>
      println(s"Got ${rdd.collect()(0)} tagged articles")
    }

    val enrichedArticles = taggedArticles.mapPartitions { it =>
      val dbpedia = DBpedia.getConnection(dbPediaBaseUrl, lang)
      val result = it.map { a =>
        val enriched = a.annotations.map(ann => dbpedia.enrichAnnotation(ann))
        a.copy(annotations = enriched)
      }
      //      DBpedia.closePool()
      result
    }

    enrichedArticles.count().foreachRDD { rdd =>
      println(s"Got ${rdd.collect()(0)} geo located articles")
    }

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
