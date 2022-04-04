ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "SparkTwitterCollector"
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.1"

libraryDependencies += "org.twitter4j" % "twitter4j-core" % "4.0.4"
libraryDependencies += "org.twitter4j" % "twitter4j-stream" % "4.0.4"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.12" % "3.2.1" % "provided"
libraryDependencies += "org.apache.bahir" %% "spark-streaming-twitter" % "2.4.0"



