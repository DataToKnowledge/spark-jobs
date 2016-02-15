package it.dtk.sparkjobs

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.elasticsearch.spark._

/**
  * Created by fabiofumarola on 15/02/16.
  */
object NewsExtractorJob {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[4]")

    conf.set("es.nodes.wan.only", "true")
      .set("es.nodes", "192.168.99.100")

    val sc = new SparkContext(conf)

    val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
    val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")

    sc.parallelize(Seq(numbers, airports)).saveToEs("spark/docs")

    val rdd = sc.esRDD("spark/docs")

    rdd.foreach(println)

    sc.stop()
  }
}
