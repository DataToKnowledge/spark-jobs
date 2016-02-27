package it.dtk.streaming.receivers

import akka.actor.Actor
import akka.actor.Actor.Receive
import it.dtk.es.ElasticQueryTerms
import org.apache.spark.streaming.receiver.ActorHelper

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._
import scala.util._

/**
  * Created by fabiofumarola on 27/02/16.
  */
class QueryTermActor(hosts: String, indexPath: String, clusterName: String,
                     scheduleTime: FiniteDuration = 10.minutes) extends Actor with ActorHelper {

  import context.dispatcher

  val queryTermExtractor = new ElasticQueryTerms(hosts, indexPath, clusterName)
  var from = 0

  context.system.scheduler.schedule(0.millis, scheduleTime, self, "extract")

  override def receive: Receive = {
    case "extract" =>
      val s = self

      queryTermExtractor.listQueryTerms(from) onComplete {
        case Success(queryTerms) =>
          if (queryTerms.nonEmpty) {
            queryTerms.foreach(qt => store(qt))
            from += queryTerms.size
            s ! "extract"
          } else {
            log.debug("reset the from position to {}", 0)
            from = 0
          }

        case Failure(ex) =>
          log.error("got exception in {} with msg {}", s.path.name, ex.getMessage, ex)
      }
  }
}
