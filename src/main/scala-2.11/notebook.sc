import it.dtk.news._


val queries = List("furto","puglia")
val lang = "it"
val ip = "151.48.44.113"
val url = AjaxGoogleNews.generateUrls(queries,"it",ip)


print(url(0))

val results = AjaxGoogleNews.articles(url(0))
println(results.size)
println(results(0).date)