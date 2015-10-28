lazy val common = Seq(
  organization := "week9.mids",
  version := "0.1.0",
  scalaVersion := "2.10.6",
  libraryDependencies ++= Seq(
    "org.apache.spark" % "spark-streaming_2.10" % "1.5.1" % "provided",
    "org.apache.spark" % "spark-streaming-twitter_2.10" % "1.5.1",
    "org.twitter4j" % "twitter4j-stream" % "3.0.3",
    "com.typesafe" % "config" % "1.3.0"
  ),
  mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
     {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x => MergeStrategy.first
     }
  }
)

lazy val twitter_popularity = (project in file(".")).
  settings(common: _*).
  settings(
    name := "twitter_popularity",
    mainClass in (Compile, run) := Some("twitter_popularity.Main"))
