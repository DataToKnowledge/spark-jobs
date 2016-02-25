package it.dtk.jobs

/**
  * Created by fabiofumarola on 24/02/16.
  * reads data from kafka
    tags the news using dbpedia
    extracts the focus location
    saves tagged news into kafka
  */
object TagNews {

  def main(args: Array[String]) {
    if (args.isEmpty) {
      println(
        """specify: local indexPath, esNodes
          |   local: true | false
          |   indexPath: wtl/feeds
          |   esNodes: 192.168.99.100
          |
          |example
          |./bin/spark-submit \
          |  --class it.dtk.jobs.ExtractFeeds \
          |  --master spark://spark-master-0 \
          |  --executor-memory 2G \
          |  --total-executor-cores 5 \
          |  /path/to/examples.jar \
          |  false wtl/feeds es-data-1
        """.stripMargin)
      sys.exit(1)
    }


  }

}
