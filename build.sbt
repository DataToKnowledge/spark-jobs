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
    name := "sparkler",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "1.6.0",
      "org.apache.spark" %% "spark-sql" % "1.6.0",
      "org.apache.spark" %% "spark-streaming" % "1.6.0"
      //to remove
      //  "com.tribbloids.spookystuff" % "spookystuff-core" % "0.3.2",
      //  "org.apache.httpcomponents" % "httpclient" % "4.5.1"
    ),
    dependencyOverrides ++= Set(
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.4"
    ),
    defaultScalariformSettings
  ) dependsOn algocore

lazy val algocore = (project in file("./algocore"))
  .settings(commons: _*)
  .settings(name := "algocore")
