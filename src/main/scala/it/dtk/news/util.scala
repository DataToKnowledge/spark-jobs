package it.dtk.news

import java.io.ByteArrayInputStream
import java.net.URL
import java.nio.charset.Charset
import java.util.Locale

import com.intenthq.gander.Gander
import com.ning.http.client.AsyncHttpClientConfig.Builder
import com.rometools.rome.io.{SyndFeedInput, XmlReader}
import it.dtk.news.model._
import org.apache.tika.language.LanguageIdentifier
import org.apache.tika.metadata.Metadata
import org.apache.tika.parser.html.HtmlParser
import org.apache.tika.parser.pdf.PDFParser
import org.apache.tika.parser.txt.{CharsetDetector, TXTParser}
import org.apache.tika.parser.{ParseContext, Parser}
import org.apache.tika.sax.BodyContentHandler
import org.jsoup.Jsoup
import play.api.libs.ws.WSResponse
import play.api.libs.ws.ning.NingWSClient

import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try


object FeedSourceScheduler {

  val minTime: FiniteDuration = 4 minutes

  def gotException(f: SchedulerParameters): SchedulerParameters =
    f.copy(time = f.time * 2)

  def when(f: SchedulerParameters, numUrls: Int): SchedulerParameters = numUrls match {
    case -1 => f
    case x: Int if x >= 5 =>
      val nextTime = if (f.time < minTime)
        minTime
      else f.time - minTime

      f.copy(time = nextTime)

    case x: Int if x < 5 =>
      val nextTime = f.time + f.delta
      f.copy(time = nextTime)
  }
}

/**
  * Created by fabiofumarola on 31/01/16.
  */
object HttpDownloader {
  //check for configurations https://www.playframework.com/documentation/2.4.x/ScalaWS
  private val builder = new Builder().
    setFollowRedirect(true).
    setUserAgent("www.wheretolive.it")

  private val WS = new NingWSClient(builder.build())

  implicit val context = play.api.libs.concurrent.Execution.Implicits.defaultContext

  def wgetF(url: String): Future[WSResponse] = WS.url(url).withFollowRedirects(true).get()

  /**
    *
    * @param url the url to retrieve
    * @return get a future to the url using Play WS
    */
  def wget(url: String, timeout: FiniteDuration = 5.seconds): Option[WSResponse] =
    try {
      val req = WS.url(url).withFollowRedirects(true).get()
      Some(Await.result(req, timeout))
    } catch {
      case e: Exception =>
        e.printStackTrace()
        println(url)
        None
    }

  def close() = WS.close()

  override def finalize(): Unit = {
    close()
  }
}

/**
  * Extract feed usig Rome
  */
object RomeFeedHelper {

  import com.github.nscala_time.time.Imports._

  def parse(url: String, publisher: String): Seq[Article] = {
    val input = new SyndFeedInput()
    val reader = input.build(new XmlReader(new URL(url)))

    reader.getEntries.map { e =>

      val description = HtmlHelper.text(e.getDescription.getValue)

      val index = if (e.getUri.lastIndexOf("http://") != -1)
        e.getUri.lastIndexOf("http://")
      else e.getUri.lastIndexOf("https://")

      val uri = e.getUri.substring(index, e.getUri.length)
      val keywords = HtmlHelper.urlTags(uri)

      Article(
        uri = uri,
        title = e.getTitle,
        description = description,
        keywords = keywords,
        publisher = publisher,
        categories = e.getCategories.map(_.getName).toList,
        imageUrl = e.getEnclosures.map(_.getUrl).mkString(""),
        date = new DateTime(e.getPublishedDate),
        cleanedText = description
      )
    }.toList
  }
}

/**
  * Parse a string using tika
  */
object TikaHelper {

  def process(html: String, contentType: String): (String, String) = {
    val in = new ByteArrayInputStream(html.getBytes(Charset.forName("UTF-8")))
    val metadata = new Metadata
    val bodyHandler = new BodyContentHandler()
    val context = new ParseContext
    val parser = getParser(contentType)

    try {
      parser.parse(in, bodyHandler, metadata, context)
    }
    catch {
      case e: Throwable =>
      //if there is an error we don't care
    }
    finally {
      in.close()
    }

    val body = bodyHandler.toString
    (body, new LanguageIdentifier(body).getLanguage)

  }

  def getParser(contentType: String): Parser = contentType match {
    case value if value contains "html" => new HtmlParser
    case value if value contains "plain" => new TXTParser
    case value if value contains "pdf" => new PDFParser
    case _ => new HtmlParser
  }
}

/**
  * Uses Gander to extract the article from html
  */
object GanderHelper {

  import com.github.nscala_time.time.Imports._

  import TikaHelper._

  def extract(html: String) = Gander.extract(html)

  def extend(art: Article): Article = {
    val webResponse = HttpDownloader.wget(art.uri)
    webResponse
      .map(ws => Try(ws.body))
      .filter(t => t.isSuccess)
      .map(_.get)
      .flatMap(body => GanderHelper.extract(body))
      .map { page =>
        val description = if (page.metaDescription.nonEmpty)
          page.metaDescription
        else art.description

        val date = page.publishDate
          .map(d => new DateTime(d.getTime)).getOrElse(art.date)

        art.copy(
          description = description,
          keywords = art.keywords ++ page.metaKeywords.split("[,\\s]+").filter(_.length > 0),
          date = date,
          lang = page.lang.getOrElse(""),
          cleanedText = page.cleanedText.getOrElse("")
        )
      }.getOrElse {
      if (webResponse.nonEmpty) {
//        val contentType = webResponse.get
//          .header("Content-Type")
//          .map(_.replace(",text/html", ""))
//          .getOrElse("")
        val (body, lang) = process(webResponse.get.body, "")
        art.copy(cleanedText = body, lang = lang)
      } else art
    }
  }
}

/**
  * General HTML helper
  */
object HtmlHelper {
  def text(html: String): String =
    Jsoup.parse(html).text()

  def host(url: String): Option[String] = {
    Try {
      val uri = new URL(url)
      s"${uri.getProtocol}://${uri.getHost}"
    }.toOption
  }

  def siteName(url: String): Option[String] = {
    Try {
      val uri = new URL(url)
      uri.getHost
    }.toOption
  }

  def urlTags(url: String): List[String] = {
    Try {
      val uri = new URL(url)
      uri.getPath.split("/|-|_").filter(_.length > 0).filterNot(_.contains(".")).toList
    } getOrElse List.empty[String]
  }

  def findRss(url: String): Set[String] = {
    Try {
      val doc = Jsoup.connect(url).get()
      val elements = doc
        .select("link[type]")
        .filter(e => e.attr("type") == "application/rss+xml").toList
      elements.map(_.attr("abs:href"))
    }.getOrElse(List.empty[String]).toSet
  }
}

/**
  * Used to find news based on term queries
  */
object AjaxGoogleNews {

  import java.text.SimpleDateFormat
  import com.github.nscala_time.time.Imports._
  import org.json4s._
  import org.json4s.jackson.JsonMethods._

  val dateFormatter = new SimpleDateFormat("E, d MMM yyyy HH:mm:ss Z", Locale.US)

  implicit val formats = org.json4s.DefaultFormats ++ org.json4s.ext.JodaTimeSerializers.all

  case class Image(url: String,
                   tbUrl: String,
                   originalContextUrl: String,
                   publisher: String,
                   tbWidth: Int,
                   tbHeight: Int)

  case class SearchResult(content: String,
                          unescapedUrl: String,
                          url: String,
                          titleNoFormatting: String,
                          publisher: String,
                          publishedDate: String,
                          language: String,
                          image: Option[Image]
                         )


  def decode(url: String): String =
    java.net.URLDecoder.decode(url, "UTF-8")

//  val example = "https://ajax.googleapis.com/ajax/services/search/news?v=1.0&q=furti%20puglia&hl=it&rsz=8&scoring=d&start=0"

  /**
    *
    * @param query
    * @param lang
    * @param ipAddress
    * @return a List of generate urls with the given term queries
    */
  def generateUrls(query: List[String], lang: String, ipAddress: String): Seq[String] = {
    val urlQuery = query.mkString("%20")
    val starts = (0 until 8).map(_ * 8)

    val baseUrl = s"https://ajax.googleapis.com/ajax/services/search/news?v=1.0" +
      s"&q=$urlQuery&hl=$lang&rsz=8&scoring=d&userip=$ipAddress&start="
    starts.map(start => baseUrl + start)
  }

  /**
    *
    * @param url
    * @return return a list of search Results
    */
  def getResults(url: String): List[SearchResult] = {
    HttpDownloader.wget(url).map { res =>
      val json = parse(res.body)
      (json \ "responseData" \ "results").extract[List[SearchResult]]
    }.getOrElse(List.empty[SearchResult])
  }

  def getResultsAsArticles(url: String): List[Article] =
    getResults(url).map { res =>

      val date = Try(dateFormatter.parse(res.publishedDate))
        .map(d => new DateTime(d))
        .getOrElse(DateTime.now())

      Article(
        uri = decode(res.unescapedUrl),
        title = res.titleNoFormatting,
        description = res.content,
        categories = List.empty,
        keywords = HtmlHelper.urlTags(res.unescapedUrl),
        imageUrl = res.image.map(_.url).getOrElse(""),
        publisher = res.publisher,
        date = date,
        lang = res.language,
        cleanedText = res.content
      )
    }
}

