lazy val commons = Seq(
  organization := "it.datatoknowledge",
  version := "0.1.0",
  scalaVersion := "2.10.6",
  scalacOptions ++= Seq("-target:jvm-1.7"), //, "-feature"
  resolvers ++= Seq(
    "spray repo" at "http://repo.spray.io",
    Resolver.sonatypeRepo("public"),
    Resolver.typesafeRepo("releases")
  )
)

lazy val root = (project in file("."))
  .settings(commons: _*)
  .settings(
    name := "spark-jobs",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "1.6.0" % "provided",
      "org.apache.spark" %% "spark-sql" % "1.6.0" % "provided",
      "org.apache.spark" %% "spark-streaming" % "1.6.0" % "provided",
      "org.apache.spark" %% "spark-streaming-kafka" % "1.6.0" % "provided",
      "org.apache.spark" %% "spark-streaming-twitter" % "1.6.0" % "provided",
      "org.elasticsearch" %% "elasticsearch-spark" % "2.2.0",
      "com.gensler" %% "scalavro" % "0.6.2"
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

assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
  case m if m.startsWith("META-INF") => MergeStrategy.discard
  case PathList("javax", "servlet", xs@_*) => MergeStrategy.first
  case PathList("org", "apache", xs@_*) => MergeStrategy.first
  case PathList("io", "netty", xs@_*) => MergeStrategy.first
  case PathList("org", "jboss", xs@_*) => MergeStrategy.first
  case "about.html" => MergeStrategy.rename
  case "reference.conf" => MergeStrategy.concat
  case _ => MergeStrategy.last
}
