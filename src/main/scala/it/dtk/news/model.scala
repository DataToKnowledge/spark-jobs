package it.dtk.news

import org.joda.time.DateTime

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._
import scala.language.postfixOps

object model {

  case class SchedulerParameters(
                                  time: FiniteDuration = 10 minutes,
                                  delta: FiniteDuration = 2 minutes)

  case class Feed(url: String,
                  publisher: String,
                  parsedUrls: List[String],
                  lastTime: Option[DateTime],
                  count: Long = 0,
                  schedulerParams: SchedulerParameters = SchedulerParameters())

  case class Article(uri: String,
                     title: String,
                     description: String,
                     categories: List[String],
                     keywords: Seq[String] = List.empty,
                     imageUrl: String,
                     publisher: String,
                     date: DateTime,
                     lang: String = "",
                     cleanedText: String = ""
                    )

}
