ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "Problem2"
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.1"