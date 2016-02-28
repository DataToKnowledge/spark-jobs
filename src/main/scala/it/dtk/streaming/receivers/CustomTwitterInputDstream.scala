package it.dtk.streaming.receivers

import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import twitter4j._
import twitter4j.auth.{Authorization, OAuthAuthorization}
import twitter4j.conf.ConfigurationBuilder
/**
  * Created by fabiana on 2/28/16.
  */
class CustomTwitterInputDstream (
                                  ssc_ : StreamingContext,
                                  twitterAuth: Option[Authorization],
                                  filters: Seq[String] = Nil,
                                  usersID: Seq[Long] = Nil,
                                  storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
                                ) extends ReceiverInputDStream[Status](ssc_)  {

  private def createOAuthAuthorization(): Authorization = {
    new OAuthAuthorization(new ConfigurationBuilder().build())
  }

  private val authorization = twitterAuth.getOrElse(createOAuthAuthorization())

  override def getReceiver(): Receiver[Status] = {
    new CustomTwitterReceiver(authorization, filters, usersID, storageLevel)
  }
}

class CustomTwitterReceiver(
                             twitterAuth: Authorization,
                             filters: Seq[String],
                             usersID: Seq[Long],
                             storageLevel: StorageLevel
                           ) extends Receiver[Status](storageLevel) with Logging {

  @volatile private var twitterStream: TwitterStream = _
  @volatile private var stopped = false

  def onStart() {
    try {
      val newTwitterStream = new TwitterStreamFactory().getInstance(twitterAuth)
      newTwitterStream.addListener(new StatusListener {
        def onStatus(status: Status): Unit = {
          store(status)
        }
        // Unimplemented
        def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice) {}
        def onTrackLimitationNotice(i: Int) {}
        def onScrubGeo(l: Long, l1: Long) {}
        def onStallWarning(stallWarning: StallWarning) {}
        def onException(e: Exception) {
          if (!stopped) {
            restart("Error receiving tweets", e)
          }
        }
      })

      val query = new FilterQuery

      if(usersID.isEmpty & filters.isEmpty)
        newTwitterStream.sample()

      if(usersID.nonEmpty)
        query.follow(usersID: _*)

      newTwitterStream.filter(query)
      setTwitterStream(newTwitterStream)

      logInfo("Twitter receiver started")
      stopped = false
    } catch {
      case e: Exception => restart("Error starting Twitter stream", e)
    }
  }

  def onStop() {
    stopped = true
    setTwitterStream(null)
    logInfo("Twitter receiver stopped")
  }

  private def setTwitterStream(newTwitterStream: TwitterStream) = synchronized {
    if (twitterStream != null) {
      twitterStream.shutdown()
    }
    twitterStream = newTwitterStream
  }
}