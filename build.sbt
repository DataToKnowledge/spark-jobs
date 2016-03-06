lazy val commons = Seq(
  organization := "it.datatoknowledge",
  version := "0.1.0",
  scalaVersion := "2.10.6",
  scalacOptions ++= Seq("-target:jvm-1.7"), //, "-feature"
  resolvers ++= Seq(
    "spray repo" at "http://repo.spray.io",
    Resolver.sonatypeRepo("public"),
    Resolver.typesafeRepo("releases"),
    "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
  )
)

lazy val root = (project in file("."))
  .settings(commons: _*)
  .settings(
    name := "spark-jobs",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "1.6.0" % "provided",
      "org.apache.spark" %% "spark-sql" % "1.6.0" % "provided",
      "org.apache.spark" %% "spark-streaming" % "1.6.0",
      "org.apache.spark" %% "spark-streaming-kafka" % "1.6.0",
      "org.apache.spark" %% "spark-streaming-twitter" % "1.6.0",
      "org.elasticsearch" %% "elasticsearch-spark" % "2.2.0",
      "com.gensler" %% "scalavro" % "0.6.2",
      "dibbhatt" % "kafka-spark-consumer" % "1.0.6"
    ),
    libraryDependencies ~= {
      _.map(_.exclude("org.slf4j", "slf4j-log4j12"))
    },
    defaultScalariformSettings
  ) dependsOn algocore

lazy val algocore = (project in file("./algocore"))
  .settings(commons: _*)
  .settings(name := "algocore")
  .dependsOn(gander)

lazy val gander = (project in file("./gander"))
  .settings(commons: _*)
  .settings(name := "gander")



//assemblyShadeRules in assembly := Seq(
//  ShadeRule.rename("org.apache.kafka.**" -> "shadeio.@1").inAll
//)


assemblyOption in assembly :=
  (assemblyOption in assembly).value.copy(includeScala = false)

assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
  case m if m.startsWith("META-INF") => MergeStrategy.discard
  case "about.html" => MergeStrategy.rename
  case "reference.conf" => MergeStrategy.concat
  case PathList("com", "github", xs@_*) => MergeStrategy.last
  case PathList("org", "elasticsearch", xs@_*) => MergeStrategy.last
  case PathList("play", "api", xs@_*) => MergeStrategy.last
  case PathList("org", "openxmlformats", xs@_*) => MergeStrategy.last
  case PathList("scala", xs@_*) => MergeStrategy.last
  case PathList("org", "apache", "lucene", xs@_*) => MergeStrategy.last
  case PathList("com", "fasterxml", "jackson", xs@_*) => MergeStrategy.last
  case PathList("play", "core", xs@_*) => MergeStrategy.last
  case PathList("com", "beust", xs@_*) => MergeStrategy.last
  case PathList("org", "bouncycastle", xs@_*) => MergeStrategy.last
  case PathList("org", "apache", "cxf", xs@_*) => MergeStrategy.last
  case PathList("org", "apache", "xerces", xs@_*) => MergeStrategy.last
  case PathList("org", "apache", "poi", xs@_*) => MergeStrategy.last
  case e if e.endsWith("xsb") => MergeStrategy.last
  case PathList("org", "apache", "hadoop", xs@_*) => MergeStrategy.discard
  case PathList("org", "apache", "spark", xs@_*) => MergeStrategy.last
  case PathList("org", "apache", "ivy", xs@_*) => MergeStrategy.last
  case PathList("org", "apache", "zookeeper", xs@_*) => MergeStrategy.last
  case PathList("org", "apache", "tika", xs@_*) => MergeStrategy.last
  case PathList("org", "apache", "http", xs@_*) => MergeStrategy.last
  case PathList("org", "apache", "commons", xs@_*) => MergeStrategy.last
  case PathList("org", "apache", "xmlbeans", xs@_*) => MergeStrategy.last
  case PathList("org", "apache", "parquet", xs@_*) => MergeStrategy.last
  case PathList("org", "apache", "log4j", xs@_*) => MergeStrategy.last
  case PathList("org", "spark-project", xs@_*) => MergeStrategy.last
  case PathList("javax", "ws", xs@_*) => MergeStrategy.last
  case PathList("twitter4j", xs@_*) => MergeStrategy.last
  case PathList("tachyon", xs@_*) => MergeStrategy.last
  case PathList("com","uwyn", xs@_*) => MergeStrategy.last
  case PathList("net","sf", xs@_*) => MergeStrategy.last
  case PathList("akka", xs@_*) => MergeStrategy.last
  case PathList("com","sksamuel", xs@_*) => MergeStrategy.last
  case PathList("com", xs@_*) => MergeStrategy.last
  case PathList("shaded", xs@_*) => MergeStrategy.last
  case PathList("org","scalactic", xs@_*) => MergeStrategy.last
  case PathList("org","jdom2", xs@_*) => MergeStrategy.last
  case PathList("views", "html", xs@_*) => MergeStrategy.discard
  case PathList("org","joda", xs@_*) => MergeStrategy.last
  case PathList("ucar","nc2", xs@_*) => MergeStrategy.last
  case PathList("it","dtk", xs@_*) => MergeStrategy.last
  case PathList("com","google", xs@_*) => MergeStrategy.last
  case PathList("org","objenesis", xs@_*) => MergeStrategy.last
  case PathList("org","jboss","netty", xs@_*) => MergeStrategy.last
  case PathList("org","jets3t", xs@_*) => MergeStrategy.last
  case PathList("com","ctc", xs@_*) => MergeStrategy.last
  case PathList("org","codehaus", xs@_*) => MergeStrategy.last
  case PathList("io","netty", xs@_*) => MergeStrategy.last
  case PathList("spray","json", xs@_*) => MergeStrategy.last
  case PathList("org","dom4j", xs@_*) => MergeStrategy.last
  case PathList("org","xerial", xs@_*) => MergeStrategy.last
  case PathList("org","apache", "kafka", "clients", xs@_*) => MergeStrategy.first
  case e =>
    // println("====== START")
    // println(e)
    // println("====== END")
    // System.exit(1)
    MergeStrategy.last
}
