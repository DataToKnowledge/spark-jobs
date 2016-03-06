package it.dtk.streaming

import java.io.ByteArrayInputStream

import akka.actor.Props
import com.gensler.scalavro.types.AvroType
import it.dtk.kafka.ConsumerProperties
import it.dtk.model.Article
import it.dtk.streaming.receivers.avro.KafkaArticleActorAvro
import kafka.serializer.DefaultDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by fabiofumarola on 28/02/16.
  */
object ArticleToElastic extends StreamUtils {

  def main(args: Array[String]) {
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
    val indexPath = "wtl/articles"
    val topic = "articles"

    var esIPs = "192.168.99.100"
    var kafkaBrokers = "192.168.99.100:9092"

    val conf = new SparkConf()
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

    val ssc = new StreamingContext(conf, Seconds(10))

    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> kafkaBrokers,
      "auto.offset.reset" -> "smallest")

    val inputStream = KafkaUtils.createDirectStream[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder](
      ssc, kafkaParams, topic.split(",").toSet
    )

    inputStream.count().foreachRDD { rdd =>
      println(s"Got ${rdd.collect()(0)} articles to index")
    }

    val articleStream = inputStream.mapPartitions { it =>
      val articleAvroType = AvroType[Article]
      it.map { kv =>
        val url = new String(kv._1)
        val article = articleAvroType.io.read(new ByteArrayInputStream(kv._2)).toOption
        url -> article
      }
    }.filter(_._2.isDefined)
      .map(kv => kv._2.get)

    inputStream.count().foreachRDD { rdd =>
      println(s"Indexing ${rdd.collect()(0)} articles")
    }

    saveArticleToElastic(indexPath, articleStream)


    ssc.start()
    ssc.awaitTermination()
  }

}
