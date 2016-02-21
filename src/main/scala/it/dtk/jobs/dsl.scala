package it.dtk.jobs

import it.dtk._

/**
  * Created by fabiofumarola on 21/02/16.
  */
object dsl {
  val web = HttpDownloader
  val feedExtr = RomeFeedHelper
  val tika = TikaHelper
  val gander = GanderHelper
  val html = HtmlHelper
  val googleNews = AjaxGoogleNews
}
