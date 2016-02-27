package it.dtk.streaming

import akka.actor.{Props, Actor}
import akka.actor.Actor.Receive
import akka.event.Logging
import it.dtk.es.ElasticFeeds
import org.apache.spark.streaming.receiver.ActorHelper
import scala.concurrent.duration._
import akka.pattern.pipe

import scala.util._

object ElasticActorReceiver {

}

/**
  * Created by fabiofumarola on 27/02/16.
  */
class ElasticActorReceiver(hosts: String, indexPath: String, clusterName: String,
                           scheduleTime: FiniteDuration = 10.minutes) extends Actor with ActorHelper {

  import context.dispatcher

  val feedExtractor = new ElasticFeeds(hosts, indexPath, clusterName)
  var from = 0

  context.system.scheduler.schedule(0.milliseconds, scheduleTime, self, "extract")

  override def receive: Receive = {
    case "extract" =>
      val s = self

      feedExtractor.feedsFuture(from) onComplete {
        case Success(feeds) =>
          if (feeds.nonEmpty) {
            feeds.foreach(i => store(i))
            from += feeds.size
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
