lazy val commons = Seq(
  organization := "it.datatoknowledge",
  version := "0.1.0",
  scalaVersion := "2.11.7",
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
      "org.apache.spark" %% "spark-core" % "1.6.0",
      "org.apache.spark" %% "spark-sql" % "1.6.0",
      "org.apache.spark" %% "spark-streaming" % "1.6.0",
      "org.apache.spark" %% "spark-streaming-kafka" % "1.6.0",
      "org.apache.spark" %% "spark-streaming-twitter" % "1.6.0",
      "org.elasticsearch" %% "elasticsearch-spark" % "2.2.0"
    ),
    libraryDependencies ~= {
      _.map(_.exclude("org.slf4j", "slf4j-log4j12"))
    },
    defaultScalariformSettings
  ) dependsOn algocore

lazy val algocore = (project in file("./algocore"))
  .settings(commons: _*)
  .settings(name := "algocore")

